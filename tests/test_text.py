import json

import pytest

from feishu_llm_bot.text import (
    markdown_post_content,
    parse_image_content,
    parse_text_content,
    split_markdown_reply,
    split_reply,
)


def test_parse_text_content() -> None:
    assert parse_text_content(json.dumps({"text": "  hello  "})) == "hello"
    assert parse_text_content("not json") is None
    assert parse_text_content(json.dumps({"text": "   "})) is None
    assert parse_text_content(json.dumps({"other": "hello"})) is None


def test_parse_image_content() -> None:
    assert parse_image_content(json.dumps({"image_key": "img_v3_safe"})) == "img_v3_safe"
    assert parse_image_content("not json") is None
    assert parse_image_content(json.dumps({"image_key": " bad"})) is None
    assert parse_image_content(json.dumps({"image_key": "bad\nkey"})) is None


def test_split_reply_prefers_boundaries() -> None:
    chunks = split_reply("first paragraph\n\nsecond paragraph", 18)
    assert chunks == ["first paragraph", "second paragraph"]


def test_split_reply_hard_splits_long_text() -> None:
    assert split_reply("abcdefghij", 4) == ["abcd", "efgh", "ij"]
    assert all(len(chunk) <= 4 for chunk in split_reply("abcdefghij", 4))


def test_split_reply_empty_and_invalid_limit() -> None:
    assert split_reply("  ", 5) == ["（模型未返回文本内容）"]
    with pytest.raises(ValueError):
        split_reply("hello", 0)


def test_markdown_reply_preserves_fences_and_limits() -> None:
    markdown = "# Header\n\n```python\n" + "print('hello')\n" * 8 + "```\n\nDone"
    chunks = split_markdown_reply(markdown, 55)
    assert all(len(chunk) <= 55 for chunk in chunks)
    assert "print('hello')" in "\n".join(chunks)


def test_markdown_reply_bounds_single_long_line() -> None:
    chunks = split_markdown_reply("x" * 25, 8)
    assert "".join(chunks) == "x" * 25
    assert all(len(chunk) <= 8 for chunk in chunks)


def test_markdown_post_content_is_unwrapped_locale_map() -> None:
    native = json.loads(markdown_post_content("# Title\n\n**bold**", native=True))
    assert set(native) == {"zh_cn"}
    assert native["zh_cn"]["content"][0][0]["tag"] == "md"
    assert "post" not in native

    structured = json.loads(markdown_post_content("**bold**", native=False))
    assert structured["zh_cn"]["content"][0][0]["style"] == ["bold"]
