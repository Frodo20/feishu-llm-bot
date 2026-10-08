"""Feishu ingress and controls survive worker/model failures."""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import signal
import threading
import time
import uuid
from pathlib import Path

from .attachments import AttachmentStore
from .bridge import configure_logging
from .card_delivery import CardPager
from .config import Settings
from .feishu import FeishuClient, FeishuEventHandler, build_websocket_client
from .health import websocket_connected
from .permission_relay import PermissionRelay
from .resident import DEFAULT_CONFIG, ensure_no_receiver
from .runtime_common import load_config, notify, private_json, singleton
from .runtime_store import RuntimeStore
from .service import SessionBridgeService
from .task_controls import TaskControls

LOGGER = logging.getLogger(__name__)


def run(config_path):
    config, env = load_config(config_path)
    settings = Settings.from_env(env)
    configure_logging(settings.app_id, settings.app_secret)
    state_dir = Path(config["state_dir"])
    receiver_lock = (Path.home() / ".local/state/feishu-bot-receivers"
                     / (hashlib.sha256(settings.app_id.encode()).hexdigest() + ".lock"))
    with singleton(state_dir / "gateway.lock"), singleton(receiver_lock):
        if settings.permission_socket_path:
            ensure_no_receiver(settings.permission_socket_path)
        store = RuntimeStore(settings.database_path)
        client = FeishuClient(app_id=settings.app_id, app_secret=settings.app_secret, timeout=5)
        attachments = AttachmentStore(
            settings.attachment_path,
            max_image_bytes=settings.max_image_bytes,
            max_image_pixels=settings.max_image_pixels,
            max_image_side=settings.max_image_side,
            max_total_bytes=settings.max_attachment_bytes_total,
        )
        service = SessionBridgeService(
            store=store,
            replies=client,
            emit_inbound=lambda _event: None,
            reply_chunk_chars=settings.reply_chunk_chars,
            max_inbound_chars=settings.max_inbound_chars,
            queue_size=settings.queue_size,
            attachments=attachments,
            progress_state_path=settings.progress_state_path,
            dispatch_enabled=False,
        )
        relay = (
            PermissionRelay(
                store=store,
                replies=client,
                socket_path=settings.permission_socket_path,
                session_id=settings.permission_session_id or config["session_id"],
                chat_id=settings.permission_chat_id,
                timeout_seconds=settings.permission_timeout_seconds,
                max_pending=settings.permission_max_pending,
                all_sessions=True,
                worker_only=True,
                cards_enabled=settings.permission_cards_enabled,
                allowed_sender_open_id=settings.allowed_sender_open_id,
                app_id=settings.app_id,
            )
            if settings.permission_relay_enabled
            else None
        )
        pager = CardPager(
            settings.progress_state_path,
            app_id=settings.app_id,
            allowed_sender=settings.allowed_sender_open_id,
        )
        task_controls = TaskControls(
            store, settings.progress_state_path, settings.app_id, settings.allowed_sender_open_id
        )

        def control(message):
            return store.handle_control(message) or bool(relay and relay.handle_control(message))

        def accept(message):
            with store._lock:
                if store._connection.execute(
                    "SELECT 1 FROM events WHERE message_id=?", (message.message_id,)
                ).fetchone():
                    return False
                pending = store._connection.execute(
                    "SELECT count(*) FROM events "
                    "WHERE status IN ('accepted','acquiring','delivered','dispatching')"
                ).fetchone()[0]
            if pending >= settings.queue_size:
                service.reject_message(
                    message.message_id, "任务队列已满，请稍后重试；/status 仍可查询。"
                )
                return False
            return service.accept(message)

        handler = FeishuEventHandler(
            allowed_sender_open_id=settings.allowed_sender_open_id,
            sink=accept,
            control_handler=control,
            reject_message=service.reject_message,
        )

        def card_callback(data):
            value = getattr(getattr(getattr(data, "event", None), "action", None), "value", None)
            if isinstance(value, dict) and value.get("kind") == "progress_page":
                return pager.handle(data)
            if isinstance(value, dict) and value.get("kind") == "task_control":
                return task_controls.handle(data)
            return relay.handle_card(data) if relay else None

        websocket = build_websocket_client(
            app_id=settings.app_id,
            app_secret=settings.app_secret,
            callback=handler.handle,
            card_callback=card_callback,
        )
        stopped = threading.Event()
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: stopped.set())
        if not config.get("runtime_enabled"):
            raise ValueError("Independent gateway requires a migrated runtime configuration")
        service.start()
        if relay:
            relay.start()
        receiver = threading.Thread(target=websocket.start, name="feishu-receiver", daemon=True)
        receiver.start()
        instance = uuid.uuid4().hex
        private_json(
            state_dir / "resident.json", {"instance": instance, "gateway_pid": os.getpid()}
        )
        notify("READY=1")
        LOGGER.info("Independent Feishu gateway started")
        try:
            while not stopped.wait(2):
                if not receiver.is_alive():
                    raise RuntimeError("Feishu receiver exited")
                now = time.time()
                private_json(
                    state_dir / "health.json",
                    {
                        "instance": instance,
                        "ready": True,
                        "timestamp": now,
                        "python_health_at": now,
                        "websocket_connected": websocket_connected(websocket),
                        "bridge_pid": os.getpid(),
                    },
                )
                # A slow acquisition must never hold all other accepted messages.
                with store._lock:
                    store._connection.execute(
                        "UPDATE events SET status='failed',"
                        "failure_reason='image_acquisition_timeout',updated_at=? "
                        "WHERE status='acquiring' AND updated_at<?",
                        (int(now), int(now - 180)),
                    )
                notify()
        finally:
            if relay:
                relay.stop()
            service.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    run(parser.parse_args().config)


if __name__ == "__main__":
    main()
