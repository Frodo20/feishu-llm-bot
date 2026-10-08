from __future__ import annotations

import hashlib
import io
import os
import stat
from pathlib import Path

import pytest

Image = pytest.importorskip("PIL.Image")

import feishu_llm_bot.attachments as attachments  # noqa: E402
from feishu_llm_bot.attachments import AttachmentStore  # noqa: E402


def image_bytes(
    image_format: str = "PNG",
    *,
    size: tuple[int, int] = (2, 2),
    color: tuple[int, int, int] = (20, 40, 60),
) -> bytes:
    output = io.BytesIO()
    Image.new("RGB", size, color).save(output, format=image_format)
    return output.getvalue()


def make_store(root: Path, **overrides: int) -> AttachmentStore:
    limits = {
        "max_image_bytes": 1024 * 1024,
        "max_image_pixels": 1_000_000,
        "max_image_side": 1_000,
        "max_total_bytes": 2 * 1024 * 1024,
    }
    limits.update(overrides)
    return AttachmentStore(root, **limits)


def token(number: int) -> str:
    return f"att_{number:032x}"


@pytest.mark.parametrize(
    ("image_format", "mime_type"),
    [("JPEG", "image/jpeg"), ("PNG", "image/png"), ("WEBP", "image/webp")],
)
def test_save_and_read_supported_images(tmp_path: Path, image_format: str, mime_type: str) -> None:
    data = image_bytes(image_format)
    store = make_store(tmp_path / "attachments")

    metadata = store.save(token(1), data)

    assert metadata.token == token(1)
    assert metadata.mime_type == mime_type
    assert metadata.byte_size == len(data)
    assert metadata.sha256 == hashlib.sha256(data).hexdigest()
    assert (metadata.width, metadata.height) == (2, 2)
    assert (
        store.read(token(1), expected_size=metadata.byte_size, expected_sha256=metadata.sha256)
        == data
    )
    root_mode = stat.S_IMODE((tmp_path / "attachments").stat().st_mode)
    file_mode = stat.S_IMODE((tmp_path / "attachments" / token(1)).stat().st_mode)
    assert root_mode == 0o700
    assert file_mode == 0o600


def test_constructor_rejects_invalid_limits(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="positive integer"):
        make_store(tmp_path / "zero", max_image_bytes=0)
    with pytest.raises(ValueError, match="positive integer"):
        make_store(tmp_path / "bool", max_image_pixels=True)


def test_constructor_rejects_non_directory_and_symlink_roots(tmp_path: Path) -> None:
    plain_file = tmp_path / "plain-file"
    plain_file.write_bytes(b"x")
    with pytest.raises((NotADirectoryError, FileExistsError)):
        make_store(plain_file)

    with pytest.raises(FileNotFoundError, match="parent"):
        make_store(tmp_path / "missing-parent" / "attachments")

    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    symlink = tmp_path / "symlink"
    symlink.symlink_to(target, target_is_directory=True)
    with pytest.raises((NotADirectoryError, ValueError, OSError)):
        make_store(symlink)


def test_constructor_rejects_insecure_root_permissions(tmp_path: Path) -> None:
    root = tmp_path / "attachments"
    root.mkdir(mode=0o700)
    root.chmod(0o750)

    with pytest.raises(PermissionError, match="0700"):
        make_store(root)


def test_constructor_rejects_root_not_owned_by_current_uid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "attachments"
    root.mkdir(mode=0o700)
    actual_uid = os.getuid()
    monkeypatch.setattr(attachments.os, "getuid", lambda: actual_uid + 1)

    with pytest.raises(PermissionError, match="current user"):
        make_store(root)


@pytest.mark.parametrize(
    "bad_token",
    [
        "att_0123",
        "att_0000000000000000000000000000000G",
        "ATT_00000000000000000000000000000000",
        "../att_00000000000000000000000000000000",
        "att_00000000000000000000000000000000/extra",
    ],
)
def test_tokens_are_strictly_validated(tmp_path: Path, bad_token: str) -> None:
    store = make_store(tmp_path / "attachments")

    with pytest.raises(ValueError, match="token"):
        store.save(bad_token, image_bytes())
    with pytest.raises(ValueError, match="token"):
        store.read(bad_token, expected_size=1, expected_sha256="0" * 64)
    with pytest.raises(ValueError, match="token"):
        store.delete(bad_token)


def test_save_rejects_empty_non_bytes_oversized_corrupt_and_unsupported_data(
    tmp_path: Path,
) -> None:
    png = image_bytes()
    store = make_store(tmp_path / "attachments", max_image_bytes=len(png) - 1)

    with pytest.raises(TypeError, match="bytes"):
        store.save(token(1), bytearray(png))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="empty"):
        store.save(token(1), b"")
    with pytest.raises(ValueError, match="byte limit"):
        store.save(token(1), png)

    regular_store = make_store(tmp_path / "other")
    with pytest.raises(ValueError, match="invalid or unsupported"):
        regular_store.save(token(1), b"not an image")
    with pytest.raises(ValueError, match="invalid or unsupported"):
        regular_store.save(token(1), image_bytes("GIF"))


def test_save_rejects_animated_webp(tmp_path: Path) -> None:
    output = io.BytesIO()
    frames = [
        Image.new("RGB", (2, 2), (10, 20, 30)),
        Image.new("RGB", (2, 2), (40, 50, 60)),
    ]
    frames[0].save(
        output,
        format="WEBP",
        save_all=True,
        append_images=frames[1:],
        duration=100,
        loop=0,
    )
    store = make_store(tmp_path / "attachments")

    with pytest.raises(ValueError, match="invalid or unsupported"):
        store.save(token(1), output.getvalue())


def test_save_rejects_truncated_image(tmp_path: Path) -> None:
    data = image_bytes("PNG")
    store = make_store(tmp_path / "attachments")

    with pytest.raises(ValueError, match="invalid or unsupported"):
        store.save(token(1), data[: len(data) // 2])


def test_save_enforces_side_and_pixel_limits(tmp_path: Path) -> None:
    data = image_bytes(size=(3, 2))
    side_store = make_store(tmp_path / "side", max_image_side=2)
    pixel_store = make_store(tmp_path / "pixels", max_image_pixels=5)

    with pytest.raises(ValueError, match="invalid or unsupported"):
        side_store.save(token(1), data)
    with pytest.raises(ValueError, match="invalid or unsupported"):
        pixel_store.save(token(1), data)


def test_save_enforces_aggregate_quota(tmp_path: Path) -> None:
    first = image_bytes(color=(1, 2, 3))
    second = image_bytes(color=(4, 5, 6))
    store = make_store(tmp_path / "attachments", max_total_bytes=len(first) + len(second) - 1)
    store.save(token(1), first)

    with pytest.raises(ValueError, match="aggregate"):
        store.save(token(2), second)
    assert not (tmp_path / "attachments" / token(2)).exists()


def test_duplicate_token_does_not_replace_original(tmp_path: Path) -> None:
    original = image_bytes(color=(1, 2, 3))
    replacement = image_bytes(color=(4, 5, 6))
    store = make_store(tmp_path / "attachments")
    metadata = store.save(token(1), original)

    with pytest.raises(FileExistsError):
        store.save(token(1), replacement)
    assert (
        store.read(token(1), expected_size=len(original), expected_sha256=metadata.sha256)
        == original
    )


def test_read_rejects_wrong_expected_size_and_digest(tmp_path: Path) -> None:
    data = image_bytes()
    store = make_store(tmp_path / "attachments")
    metadata = store.save(token(1), data)

    with pytest.raises(ValueError, match="size mismatch"):
        store.read(token(1), expected_size=metadata.byte_size - 1, expected_sha256=metadata.sha256)
    with pytest.raises(ValueError, match="SHA-256"):
        store.read(token(1), expected_size=metadata.byte_size, expected_sha256="0" * 64)
    with pytest.raises(ValueError, match="lowercase hexadecimal"):
        store.read(token(1), expected_size=metadata.byte_size, expected_sha256="A" * 64)


def test_read_detects_file_tampering(tmp_path: Path) -> None:
    data = image_bytes()
    store = make_store(tmp_path / "attachments")
    metadata = store.save(token(1), data)
    attachment = tmp_path / "attachments" / token(1)

    attachment.write_bytes(data + b"x")
    with pytest.raises(ValueError, match="size mismatch"):
        store.read(token(1), expected_size=metadata.byte_size, expected_sha256=metadata.sha256)

    attachment.write_bytes(b"x" * metadata.byte_size)
    attachment.chmod(0o600)
    with pytest.raises(ValueError, match="SHA-256"):
        store.read(token(1), expected_size=metadata.byte_size, expected_sha256=metadata.sha256)


def test_read_rejects_insecure_file_permissions(tmp_path: Path) -> None:
    data = image_bytes()
    store = make_store(tmp_path / "attachments")
    metadata = store.save(token(1), data)
    (tmp_path / "attachments" / token(1)).chmod(0o640)

    with pytest.raises(PermissionError, match="group or other"):
        store.read(token(1), expected_size=metadata.byte_size, expected_sha256=metadata.sha256)


def test_read_does_not_follow_attachment_symlinks(tmp_path: Path) -> None:
    data = image_bytes()
    root = tmp_path / "attachments"
    store = make_store(root)
    outside = tmp_path / "outside"
    outside.write_bytes(data)
    (root / token(1)).symlink_to(outside)

    with pytest.raises(OSError):
        store.read(
            token(1),
            expected_size=len(data),
            expected_sha256=hashlib.sha256(data).hexdigest(),
        )


def test_delete_is_idempotent(tmp_path: Path) -> None:
    store = make_store(tmp_path / "attachments")
    store.save(token(1), image_bytes())

    assert store.delete(token(1))
    assert not store.delete(token(1))


def test_delete_does_not_unlink_symlinks(tmp_path: Path) -> None:
    root = tmp_path / "attachments"
    store = make_store(root)
    outside = tmp_path / "outside"
    outside.write_bytes(b"keep")
    (root / token(1)).symlink_to(outside)

    with pytest.raises(ValueError, match="regular file"):
        store.delete(token(1))
    assert outside.read_bytes() == b"keep"
    assert (root / token(1)).is_symlink()


def test_cleanup_removes_only_store_temporary_files(tmp_path: Path) -> None:
    root = tmp_path / "attachments"
    store = make_store(root)
    temporary_one = root / (".tmp-" + "1" * 32)
    temporary_two = root / (".tmp-" + "a" * 32)
    unrelated = root / ".tmp-not-a-store-file"
    temporary_directory = root / (".tmp-" + "b" * 32)
    temporary_one.write_bytes(b"one")
    temporary_two.write_bytes(b"two")
    unrelated.write_bytes(b"keep")
    temporary_directory.mkdir()

    assert store.cleanup_temporary_files() == 2
    assert not temporary_one.exists()
    assert not temporary_two.exists()
    assert unrelated.exists()
    assert temporary_directory.is_dir()
    assert store.cleanup_temporary_files() == 0
