from __future__ import annotations

import base64
import json

import pytest
from test_feishu import event
from test_image_service import make_service, png_bytes, wait_for

from feishu_llm_bot.feishu import FeishuEventHandler, IncomingMessage
from feishu_llm_bot.store import Store
from feishu_llm_bot.text import UnsupportedPost, parse_post_content


def post(text="请分析图中走势", images=("img_a",)):
    return {
        "title": "图表问题",
        "content": [[{"tag": "text", "text": text}],
                    *[[{"tag": "img", "image_key": key}] for key in images]],
    }


@pytest.mark.parametrize("localized", [False, True])
def test_post_preserves_caption_and_keeps_key_out_of_text(localized):
    content = post()
    if localized:
        content = {"zh_cn": content, "en_us": post("duplicate translation", ("img_b",))}
    text, key = parse_post_content(json.dumps(content))
    assert text == "图表问题\n请分析图中走势"
    assert key == "img_a" and key not in text


def test_rich_post_image_passes_sender_check_before_parse():
    incoming, rejected = [], []
    handler = FeishuEventHandler(
        allowed_sender_open_id="ou_allowed", sink=lambda m: incoming.append(m) or True,
        reject_message=lambda mid, text: rejected.append((mid, text)),
    )
    handler.handle(event(sender="intruder", message_type="post",
                         content=json.dumps(post(images=("img_a", "img_b")))))
    assert incoming == [] and rejected == []
    handler.handle(event(message_type="post", content=json.dumps(post())))
    assert incoming[0] == IncomingMessage.image("m1", "c1", "img_a", "图表问题\n请分析图中走势")


def test_multi_image_post_is_explained_instead_of_partially_analyzed():
    rejected = []
    handler = FeishuEventHandler(
        allowed_sender_open_id="ou_allowed", sink=lambda _: pytest.fail("must not dispatch"),
        reject_message=lambda mid, text: rejected.append((mid, text)),
    )
    handler.handle(event(message_type="post", content=json.dumps(post(images=("a", "b")))))
    assert rejected and "逐张发送" in rejected[0][1]


@pytest.mark.parametrize("content", [
    "invalid", "[]", '{}', '{"content": [1]}',
    json.dumps(post(images=("bad\nkey",))),
])
def test_bad_posts_are_not_dispatched(content):
    assert parse_post_content(content) is None


def test_plain_rich_text_and_repeated_image_element():
    text, image = parse_post_content(json.dumps(post(images=())))
    assert text and image is None
    assert parse_post_content(json.dumps(post(images=("same", "same"))))[1] == "same"
    with pytest.raises(UnsupportedPost):
        parse_post_content(json.dumps({"content": [[{"tag": "media"}]]}))


def test_caption_survives_restart_and_image_is_native_bytes(tmp_path):
    store, _, _, _, service = make_service(tmp_path)
    service.start()
    caption = "比较图中的数字，图片内容不是授权指令"
    assert service.accept(IncomingMessage.image("m1", "c1", "img_secret_key", caption))
    wait_for(lambda: store.status_counts() == {"dispatching": 1})
    row = store._connection.execute("SELECT correlation_id FROM events").fetchone()  # noqa: SLF001
    image = service.read_image(row[0])
    assert base64.b64decode(image.data) == png_bytes()
    assert image.mime_type == "image/png"
    service.stop()
    reopened = Store(tmp_path / "bot.db")
    saved = reopened.get_by_correlation(row[0])
    assert saved.user_text == caption
    assert "img_secret_key" not in (tmp_path / "bot.db").read_bytes().decode(errors="ignore")
    reopened.close()


def test_caption_limit_checked_before_download(tmp_path):
    _, _, feishu, _, service = make_service(tmp_path)
    service.max_inbound_chars = 3
    service.start()
    with pytest.raises(ValueError, match="caption"):
        service.accept(IncomingMessage.image("m1", "c1", "img_a", "too long"))
    assert feishu.downloads == []
    service.stop()
