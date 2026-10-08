from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import re
import secrets
import socket
import stat
import struct
import threading
import time
import uuid
from pathlib import Path
from typing import Protocol

from lark_oapi.event.callback.model.p2_card_action_trigger import (
    P2CardActionTrigger,
    P2CardActionTriggerResponse,
)

from .cards import permission_card
from .feishu import IncomingMessage, outbound_message_uuid
from .store import PermissionRequestRecord, Store

LOGGER = logging.getLogger(__name__)
_MAX_REQUEST_BYTES = 16 * 1024
_MAX_FIELD_CHARS = 256
_REQUEST_KEYS = {"session_id", "tool_name", "cwd_context", "summary"}
_TOOL_NAME = re.compile(r"[A-Za-z0-9_.:-]{1,256}\Z")
_TOKEN = re.compile(r"[A-Za-z0-9_-]{12,64}\Z")
_COMMAND = re.compile(r"(?:同意|拒绝|/allow|/deny)[ \t]+([A-Za-z0-9_-]{12,64})\Z")
_COMMAND_PREFIX = re.compile(r"(?:同意|拒绝|/allow|/deny)(?:\s|\Z)")


class FeishuPermissionClient(Protocol):
    def send_text(self, chat_id: str, text: str, send_uuid: str) -> None: ...

    def reply_text(self, message_id: str, text: str, send_uuid: str) -> None: ...

    def send_card(self, chat_id: str, card: dict, send_uuid: str) -> str: ...

    def update_card(self, message_id: str, card: dict) -> None: ...


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("ascii")).hexdigest()


def _decision_from_command(text: str) -> tuple[str, str] | None:
    match = _COMMAND.fullmatch(text.strip())
    if match is None:
        return None
    verb = text.strip().split(maxsplit=1)[0]
    decision = "allowed" if verb in {"同意", "/allow"} else "denied"
    return decision, match.group(1)


def _has_control_prefix(text: str) -> bool:
    return _COMMAND_PREFIX.match(text.strip()) is not None


class PermissionRelay:
    def __init__(
        self,
        *,
        store: Store,
        replies: FeishuPermissionClient,
        socket_path: Path,
        session_id: str,
        chat_id: str | None,
        timeout_seconds: int,
        max_pending: int,
        all_sessions: bool = False,
        cards_enabled: bool = False,
        allowed_sender_open_id: str | None = None,
        app_id: str | None = None,
        worker_only: bool = False,
    ) -> None:
        self.store = store
        self.replies = replies
        self.socket_path = socket_path
        self.session_id = session_id
        self.chat_id = chat_id
        self.timeout_seconds = timeout_seconds
        self.max_pending = max_pending
        self.all_sessions = all_sessions
        self.cards_enabled = cards_enabled
        self.allowed_sender_open_id = allowed_sender_open_id
        self.app_id = app_id
        self.worker_only = worker_only
        if cards_enabled and (not allowed_sender_open_id or not app_id):
            raise ValueError("card approvals require an allowlisted sender and app ID")
        self._stop = threading.Event()
        self._changed = threading.Condition()
        self._listener: socket.socket | None = None
        self._accept_thread: threading.Thread | None = None
        self._workers: set[threading.Thread] = set()
        self._workers_lock = threading.Lock()

    def start(self) -> None:
        if self._accept_thread is not None:
            raise RuntimeError("permission relay cannot be started twice")
        self._prepare_socket_path()
        expired = self.store.expire_all_pending_permission_requests(now=int(time.time()))
        if expired:
            LOGGER.info("expired stale permission requests count=%d", expired)
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            listener.bind(str(self.socket_path))
            os.chmod(self.socket_path, 0o600, follow_symlinks=False)
            listener.listen(self.max_pending)
            listener.settimeout(0.5)
        except Exception:
            listener.close()
            with contextlib.suppress(FileNotFoundError):
                self.socket_path.unlink()
            raise
        self._listener = listener
        self._accept_thread = threading.Thread(
            target=self._accept_loop,
            name="feishu-permission-relay",
            daemon=True,
        )
        self._accept_thread.start()
        LOGGER.info("permission relay started")

    def stop(self, timeout: float = 15.0) -> None:
        if self._stop.is_set():
            return
        self._stop.set()
        listener = self._listener
        if listener is not None:
            listener.close()
        self.store.expire_all_pending_permission_requests(now=int(time.time()))
        with self._changed:
            self._changed.notify_all()
        deadline = time.monotonic() + timeout
        if self._accept_thread is not None:
            self._accept_thread.join(max(0.0, deadline - time.monotonic()))
        with self._workers_lock:
            workers = tuple(self._workers)
        for worker in workers:
            worker.join(max(0.0, deadline - time.monotonic()))
        alive = [worker for worker in workers if worker.is_alive()]
        if alive or (self._accept_thread is not None and self._accept_thread.is_alive()):
            raise TimeoutError("permission relay did not stop")
        with contextlib.suppress(FileNotFoundError):
            self.socket_path.unlink()
        LOGGER.info("permission relay stopped")

    def handle_control(self, message: IncomingMessage) -> bool:
        if message.message_type != "text" or message.user_text is None:
            return False
        parsed = _decision_from_command(message.user_text)
        if parsed is None:
            if not _has_control_prefix(message.user_text):
                return False
            self._send_ack(message.message_id, False)
            return True
        decision, token = parsed
        record: PermissionRequestRecord | None = None
        if self.chat_id is not None and message.chat_id == self.chat_id:
            record = self.store.resolve_permission_request(
                token_hash=_token_hash(token),
                chat_id=message.chat_id,
                resolution_message_id=message.message_id,
                decision=decision,
                now=int(time.time()),
            )
        if record is not None:
            with self._changed:
                self._changed.notify_all()
            LOGGER.info(
                "permission request resolved request_id=%s status=%s",
                record.request_id,
                record.status,
            )
        self._send_ack(message.message_id, record is not None, decision)
        return True

    def handle_card(self, data: P2CardActionTrigger) -> P2CardActionTriggerResponse:
        """Runs on the SDK callback thread: no network calls or long-lived locks."""
        invalid = {"toast": {"type": "error", "content": "授权卡片无效或不属于当前用户。"}}
        try:
            header = getattr(data, "header", None)
            event = getattr(data, "event", None)
            operator = getattr(event, "operator", None)
            context = getattr(event, "context", None)
            action = getattr(event, "action", None)
            value = getattr(action, "value", None)
            event_id = getattr(header, "event_id", None)
            message_id = getattr(context, "open_message_id", None)
            if (
                not self.cards_enabled
                or getattr(header, "app_id", None) != self.app_id
                or getattr(header, "event_type", None) != "card.action.trigger"
                or getattr(operator, "open_id", None) != self.allowed_sender_open_id
                or getattr(context, "open_chat_id", None) != self.chat_id
                or getattr(event, "host", None) != "im_message"
                or getattr(action, "tag", None) != "button"
                or not isinstance(event_id, str) or not 1 <= len(event_id) <= 256
                or not isinstance(message_id, str) or not 1 <= len(message_id) <= 256
                or not isinstance(value, dict)
                or set(value) != {"kind", "request_id", "token", "decision"}
                or value.get("kind") != "claude_permission"
                or value.get("decision") not in {"allowed", "denied"}
                or not isinstance(value.get("token"), str)
                or _TOKEN.fullmatch(value["token"]) is None
                or not isinstance(value.get("request_id"), str)
                or re.fullmatch(r"pr_[0-9a-f]{32}", value["request_id"]) is None
            ):
                return P2CardActionTriggerResponse(invalid)
            record, changed = self.store.resolve_permission_card(
                request_id=value["request_id"], token_hash=_token_hash(value["token"]),
                chat_id=self.chat_id, message_id=message_id, event_id=event_id,
                decision=value["decision"], now=int(time.time()),
            )
            if record is None:
                return P2CardActionTriggerResponse(invalid)
            if changed:
                with self._changed:
                    self._changed.notify_all()
                LOGGER.info("card permission resolved request_id=%s status=%s",
                            record.request_id, record.status)
            labels = {"allowed": "已同意本次操作", "denied": "已拒绝本次操作",
                      "expired": "请求已过期，请等待新的授权卡片"}
            text = labels.get(record.status, "请求已结束")
            if not changed and record.status != "expired":
                text = f"该请求已处理：{text}"
            return P2CardActionTriggerResponse({
                "toast": {"type": "success" if changed else "info", "content": text},
                "card": {"type": "raw", "data": permission_card(record)},
            })
        except Exception:
            LOGGER.warning("card permission callback failed")
            return P2CardActionTriggerResponse({
                "toast": {"type": "error", "content": "暂时无法处理，请稍后再次点击。"},
            })

    def _send_ack(self, message_id: str, succeeded: bool, decision: str | None = None) -> None:
        if succeeded:
            text = "已同意这一次权限请求。" if decision == "allowed" else "已拒绝这一次权限请求。"
        else:
            text = "授权指令无效、已过期或已使用。请检查最新的权限请求。"
        if self.worker_only:
            with self.store._lock:
                self.store._connection.execute(
                    "INSERT OR IGNORE INTO runtime_controls VALUES (?,?,?,0,0,0)",
                    (message_id, self.chat_id or "", text),
                )
            return
        try:
            self.replies.reply_text(
                message_id,
                text,
                outbound_message_uuid(f"permission-ack:{message_id}", 0),
            )
        except Exception:
            LOGGER.warning("failed to send permission acknowledgement")

    def _prepare_socket_path(self) -> None:
        if not self.socket_path.is_absolute():
            raise ValueError("permission socket path must be absolute")
        parent = self.socket_path.parent
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        parent_stat = parent.lstat()
        if not stat.S_ISDIR(parent_stat.st_mode) or parent_stat.st_uid != os.getuid():
            raise PermissionError("permission socket directory must be current-user-owned")
        if stat.S_IMODE(parent_stat.st_mode) & 0o077:
            raise PermissionError("permission socket directory must be owner-only")
        if parent.resolve() != parent.absolute():
            raise PermissionError("permission socket directory must not contain symlinks")
        if self.socket_path.exists() or self.socket_path.is_symlink():
            socket_stat = self.socket_path.lstat()
            if not stat.S_ISSOCK(socket_stat.st_mode) or socket_stat.st_uid != os.getuid():
                raise PermissionError("permission socket path is unsafe")
            probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                probe.settimeout(0.2)
                probe.connect(str(self.socket_path))
            except (ConnectionRefusedError, FileNotFoundError):
                self.socket_path.unlink(missing_ok=True)
            except OSError as exc:
                raise RuntimeError("cannot verify existing permission socket") from exc
            else:
                raise RuntimeError("permission relay is already running")
            finally:
                probe.close()

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            listener = self._listener
            if listener is None:
                return
            try:
                connection, _ = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                if self._stop.is_set():
                    return
                LOGGER.exception("permission relay accept failed")
                continue
            worker = threading.Thread(
                target=self._serve_connection,
                args=(connection,),
                name="feishu-permission-request",
                daemon=True,
            )
            with self._workers_lock:
                self._workers.add(worker)
            worker.start()

    def _serve_connection(self, connection: socket.socket) -> None:
        try:
            with connection:
                try:
                    self._verify_peer(connection)
                    if self.chat_id is None:
                        raise RuntimeError("permission relay chat is not established")
                    connection.settimeout(5.0)
                    request = self._read_request(connection)
                    request_id = f"pr_{secrets.token_hex(16)}"
                    token = secrets.token_urlsafe(12)
                    now = int(time.time())
                    expires_at = now + self.timeout_seconds
                    record = self.store.create_permission_request(
                        request_id=request_id,
                        session_id=request["session_id"],
                        chat_id=self.chat_id,
                        tool_name=request["tool_name"],
                        summary=request["summary"],
                        token_hash=_token_hash(token),
                        created_at=now,
                        expires_at=expires_at,
                        max_pending=self.max_pending,
                    )
                    try:
                        if self.cards_enabled:
                            message_id = self.replies.send_card(
                                self.chat_id,
                                permission_card(record, cwd_context=request["cwd_context"],
                                                token=token),
                                outbound_message_uuid(f"permission:{request_id}", 0),
                            )
                            self.store.bind_permission_card(record.request_id, message_id)
                        else:
                            self.replies.send_text(
                                self.chat_id,
                                self._approval_text(record, request["cwd_context"], token),
                                outbound_message_uuid(f"permission:{request_id}", 0),
                            )
                    except Exception:
                        self._resolve_internal(record, token, "send-failed")
                        LOGGER.warning(
                            "failed to send permission request request_id=%s", request_id
                        )
                        self._write_decision(connection, "deny")
                        return
                    LOGGER.info(
                        "permission request sent request_id=%s tool=%s",
                        request_id,
                        record.tool_name,
                    )
                    decision = self._wait_for_decision(connection, record, token)
                    try:
                        self._write_decision(connection, decision)
                    finally:
                        self._finish_card(record.request_id)
                except Exception:
                    LOGGER.warning("permission relay request failed", exc_info=True)
                    with contextlib.suppress(Exception):
                        connection.setblocking(True)
                        connection.settimeout(2.0)
                        payload = json.dumps(
                            {"decision": "deny"}, separators=(",", ":")
                        ).encode()
                        connection.sendall(payload + b"\n")
        finally:
            with self._workers_lock:
                self._workers.discard(threading.current_thread())

    def _wait_for_decision(
        self,
        connection: socket.socket,
        record: PermissionRequestRecord,
        token: str,
    ) -> str:
        deadline = time.monotonic() + self.timeout_seconds
        connection.setblocking(False)
        while not self._stop.is_set():
            current = self.store.get_permission_request(record.request_id)
            if self.worker_only:
                with self.store._lock:
                    active = self.store._connection.execute(
                        "SELECT 1 FROM runtime_attempts a "
                        "JOIN runtime_tasks t USING(correlation_id) "
                        "WHERE a.session_id=? AND a.state IN ('starting','running') "
                        "AND t.attempt_id=a.attempt_id AND t.cancel_requested=0",
                        (record.session_id,),
                    ).fetchone()
                if not active:
                    self._resolve_internal(record, token, "execution-ended")
                    return "deny"
            if current is None or current.status in {"denied", "expired"}:
                return "deny"
            if current.status == "allowed":
                return "allow"
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.store.expire_permission_requests(now=int(time.time()))
                return "deny"
            try:
                if connection.recv(1, socket.MSG_PEEK) == b"":
                    self._resolve_internal(record, token, "disconnected")
                    return "deny"
            except BlockingIOError:
                pass
            with self._changed:
                self._changed.wait(min(remaining, 0.25))
        self._resolve_internal(record, token, "shutdown")
        return "deny"

    def _finish_card(self, request_id: str) -> None:
        if not self.cards_enabled:
            return
        record = self.store.get_permission_request(request_id)
        if record is None or record.card_message_id is None or record.status == "pending":
            return
        try:
            self.replies.update_card(record.card_message_id, permission_card(record))
        except Exception:
            LOGGER.warning("failed to update resolved permission card request_id=%s", request_id)

    def _resolve_internal(self, record: PermissionRequestRecord, token: str, reason: str) -> None:
        resolved = self.store.resolve_permission_request(
            token_hash=_token_hash(token),
            chat_id=self.chat_id,
            resolution_message_id=f"internal:{reason}:{record.request_id}",
            decision="denied",
            now=int(time.time()),
        )
        if resolved is not None:
            with self._changed:
                self._changed.notify_all()

    def _read_request(self, connection: socket.socket) -> dict[str, str]:
        raw = bytearray()
        while len(raw) <= _MAX_REQUEST_BYTES:
            chunk = connection.recv(min(4096, _MAX_REQUEST_BYTES + 1 - len(raw)))
            if not chunk:
                break
            raw.extend(chunk)
            newline = raw.find(b"\n")
            if newline >= 0:
                if newline != len(raw) - 1:
                    raise ValueError("permission request has trailing data")
                break
        if len(raw) > _MAX_REQUEST_BYTES:
            raise ValueError("permission request exceeds the limit")
        if not raw.endswith(b"\n"):
            raise ValueError("permission request is incomplete")
        try:
            request = json.loads(raw[:-1])
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("permission request is not valid JSON") from exc
        keys = _REQUEST_KEYS | {"attempt_id", "token"} if self.worker_only else _REQUEST_KEYS
        if not isinstance(request, dict) or set(request) != keys:
            raise ValueError("permission request has an invalid shape")
        if self.worker_only:
            attempt = self.store.authenticate(request["attempt_id"], request["token"])
            if attempt["session_id"] != request["session_id"]:
                raise PermissionError("Permission request belongs to another execution")
        if not self.all_sessions and request.get("session_id") != self.session_id:
            raise ValueError("permission request targets a different session")
        for key in _REQUEST_KEYS:
            value = request.get(key)
            if (
                not isinstance(value, str)
                or not value
                or len(value) > _MAX_FIELD_CHARS
                or any(ord(character) < 32 for character in value)
            ):
                raise ValueError("permission request has an invalid field")
        if _TOOL_NAME.fullmatch(request["tool_name"]) is None:
            raise ValueError("permission request has an invalid tool name")
        if self.all_sessions:
            uuid.UUID(request["session_id"])
        return request

    @staticmethod
    def _write_decision(connection: socket.socket, decision: str) -> None:
        connection.setblocking(True)
        connection.settimeout(2.0)
        payload = json.dumps({"decision": decision}, separators=(",", ":")).encode() + b"\n"
        connection.sendall(payload)

    @staticmethod
    def _verify_peer(connection: socket.socket) -> None:
        if not hasattr(socket, "SO_PEERCRED"):
            return
        credentials = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
        _pid, uid, _gid = struct.unpack("3i", credentials)
        if uid != os.getuid():
            raise PermissionError("permission relay peer is not the current user")

    def _approval_text(
        self,
        record: PermissionRequestRecord,
        cwd_context: str,
        token: str,
    ) -> str:
        minutes = max(1, (self.timeout_seconds + 59) // 60)
        return "\n".join(
            (
                "Claude Code 请求一次性授权",
                f"会话：{record.session_id}",
                f"工具：{record.tool_name}",
                f"范围：{record.summary}",
                f"目录：{cwd_context}",
                f"有效期：{minutes} 分钟",
                "",
                f"同意 {token}",
                f"拒绝 {token}",
            )
        )
