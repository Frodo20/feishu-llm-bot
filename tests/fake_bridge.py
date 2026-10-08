from __future__ import annotations

import json
import os
import sys

CORRELATION_ID = "fs_0123456789abcdef0123456789abcdef"
PNG_DATA = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII="
)


def write(payload: dict[str, object]) -> None:
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def main() -> None:
    if os.environ.get("CLAUDE_CODE_MESSAGING_SOCKET"):
        raise RuntimeError("session inbox socket leaked to Python child")
    if os.environ.get("CLAUDE_CODE_MESSAGING_TOKEN"):
        raise RuntimeError("session inbox token leaked to Python child")
    if os.environ.get("FEISHU_FAKE_MALFORMED_INBOUND"):
        inbound: dict[str, object] = {
            "event": "inbound",
            "correlation_id": CORRELATION_ID,
            "message_type": "image",
            "content": "images must not have inline content",
        }
        write({"event": "ready"})
        write(inbound)
        return
    write({"event": "ready"})
    if os.environ.get("FEISHU_FAKE_MESSAGE_TYPE") == "image":
        inbound = {
            "event": "inbound",
            "correlation_id": CORRELATION_ID,
            "message_type": "image",
        }
        if os.environ.get("FEISHU_FAKE_CAPTION"):
            inbound["caption"] = os.environ["FEISHU_FAKE_CAPTION"]
    else:
        inbound = {
            "event": "inbound",
            "correlation_id": CORRELATION_ID,
            "message_type": "text",
            "content": os.environ.get("FEISHU_FAKE_INBOUND", "hello from Feishu"),
        }
    write(inbound)
    for line in sys.stdin:
        request = json.loads(line)
        if request["method"] == "mark_delivered":
            result = {"marked": True}
        elif request["method"] == "read_image":
            malformed = os.environ.get("FEISHU_FAKE_MALFORMED_IMAGE")
            if malformed == "base64":
                result = {
                    "data": "not base64!",
                    "mime_type": "image/png",
                    "byte_length": 7,
                }
            elif malformed == "mime":
                result = {
                    "data": PNG_DATA,
                    "mime_type": "image/gif",
                    "byte_length": 68,
                }
            elif malformed == "length":
                result = {
                    "data": PNG_DATA,
                    "mime_type": "image/png",
                    "byte_length": 1,
                }
            elif malformed == "oversize":
                result = {
                    "data": "A" * (((5 * 1024 * 1024) // 3 + 1) * 4),
                    "mime_type": "image/png",
                    "byte_length": 5 * 1024 * 1024 + 1,
                }
            else:
                result = {
                    "data": PNG_DATA,
                    "mime_type": "image/png",
                    "byte_length": 68,
                }
        elif request["method"] == "reply":
            result = {"status": "sent 1 part(s)"}
        else:
            write({"id": request["id"], "error": "unknown method"})
            continue
        write({"id": request["id"], "result": result})


if __name__ == "__main__":
    main()
