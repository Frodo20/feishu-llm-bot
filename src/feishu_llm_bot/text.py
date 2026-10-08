from __future__ import annotations

import json

from lark_oapi.channel.outbound.markdown import markdown_to_post_ast, split_with_code_fences

_EMPTY_REPLY = "（模型未返回文本内容）"


class UnsupportedPost(ValueError):
    """A valid private post that needs a user-facing explanation."""


def parse_post_content(content: str) -> tuple[str, str | None] | None:
    """Read a Feishu rich post without fetching links or exposing image keys as text."""
    try:
        payload = json.loads(content)
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if "content" not in payload:
        # Receive events are usually flattened; outbound/localized posts also occur.
        payload = next((payload[k] for k in ("zh_cn", "en_us", "ja_jp")
                        if isinstance(payload.get(k), dict)), None)
        if payload is None:
            return None
    rows = payload.get("content")
    if not isinstance(rows, list):
        return None
    title = payload.get("title", "")
    if not isinstance(title, str):
        return None
    lines = [title] if title else []
    images: list[str] = []
    for row in rows:
        if not isinstance(row, list):
            return None
        parts = []
        for item in row:
            if not isinstance(item, dict):
                return None
            tag = item.get("tag")
            if tag == "img":
                key = parse_image_content(json.dumps({"image_key": item.get("image_key")}))
                if key is None:
                    return None
                if key not in images:
                    images.append(key)
            elif tag in {"text", "a", "md", "code_block"}:
                text = item.get("text", "")
                if not isinstance(text, str):
                    return None
                parts.append(text)
                if tag == "a" and isinstance(item.get("href"), str):
                    parts.append(f" ({item['href']})")
            elif tag == "at":
                name = item.get("user_name", "")
                if isinstance(name, str) and name:
                    parts.append(f"@{name}")
            elif tag in {"media", "file"}:
                raise UnsupportedPost("暂不支持图文消息中的视频或文件，请直接发送图片。")
        if parts:
            lines.append("".join(parts))
    if len(images) > 1:
        raise UnsupportedPost("这条消息包含多张图片，请逐张发送，每张图片可附带问题。")
    text = "\n".join(lines).strip()
    if not text and not images:
        return None
    return text, images[0] if images else None


def parse_text_content(content: str) -> str | None:
    try:
        payload = json.loads(content)
    except (TypeError, json.JSONDecodeError):
        return None
    text = payload.get("text") if isinstance(payload, dict) else None
    if not isinstance(text, str):
        return None
    text = text.strip()
    return text or None


def parse_image_content(content: str, *, max_key_chars: int = 512) -> str | None:
    try:
        payload = json.loads(content)
    except (TypeError, json.JSONDecodeError):
        return None
    key = payload.get("image_key") if isinstance(payload, dict) else None
    if not isinstance(key, str) or not key or len(key) > max_key_chars:
        return None
    if key.strip() != key or any(
        ord(character) < 0x20 or ord(character) == 0x7F for character in key
    ):
        return None
    return key


def split_reply(text: str, limit: int) -> list[str]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    text = text.strip()
    if not text:
        return [_EMPTY_REPLY]

    chunks: list[str] = []
    remaining = text
    while len(remaining) > limit:
        window = remaining[: limit + 1]
        split_at = max(window.rfind("\n\n", 0, limit + 1), window.rfind("\n", 0, limit + 1))
        if split_at < limit // 3:
            split_at = max(window.rfind("。", 0, limit + 1), window.rfind(" ", 0, limit + 1))
        if split_at < limit // 3:
            split_at = limit
        else:
            split_at += 1
        chunks.append(remaining[:split_at].rstrip())
        remaining = remaining[split_at:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


def split_markdown_reply(text: str, limit: int) -> list[str]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    text = text.strip() or _EMPTY_REPLY
    chunks = [chunk for chunk in split_with_code_fences(text, limit) if chunk]
    if chunks and all(len(chunk) <= limit for chunk in chunks):
        return chunks

    fence_overhead = max(
        (len(line) + 5 for line in text.splitlines() if line.startswith("```")),
        default=5,
    )
    chunks = [
        chunk for chunk in split_with_code_fences(text, max(1, limit - fence_overhead)) if chunk
    ]
    if chunks and all(len(chunk) <= limit for chunk in chunks):
        return chunks

    # The SDK can overflow its advertised limit for a pathological single line.
    # Hard-slice only non-fenced Markdown; slicing a synthetic fenced chunk can
    # create invalid standalone posts, so reject it rather than sending damage.
    bounded: list[str] = []
    for chunk in chunks:
        if len(chunk) <= limit:
            bounded.append(chunk)
        elif "```" in chunk:
            raise ValueError("fenced Markdown cannot be split within the configured limit")
        else:
            bounded.extend(
                piece
                for index in range(0, len(chunk), limit)
                if (piece := chunk[index : index + limit])
            )
    return bounded or [_EMPTY_REPLY]


def markdown_post_content(markdown: str, *, native: bool = True) -> str:
    post = markdown_to_post_ast(
        markdown,
        tag_md_mode="native" if native else "structured",
    )
    return json.dumps(post, ensure_ascii=False)
