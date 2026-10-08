from __future__ import annotations

import errno
import fcntl
import hashlib
import hmac
import io
import os
import re
import secrets
import stat
import threading
import warnings
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path

from PIL import Image

_TOKEN_PATTERN = re.compile(r"att_[0-9a-f]{32}\Z")
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}\Z")
_TEMPORARY_PATTERN = re.compile(r"\.tmp-[0-9a-f]{32}\Z")
_MIME_TYPES = {
    "JPEG": "image/jpeg",
    "PNG": "image/png",
    "WEBP": "image/webp",
}
_READ_CHUNK_SIZE = 64 * 1024


@dataclass(frozen=True, slots=True)
class ImageMetadata:
    token: str
    mime_type: str
    byte_size: int
    sha256: str
    width: int
    height: int


class AttachmentStore:
    def __init__(
        self,
        root: str | os.PathLike[str],
        max_image_bytes: int,
        max_image_pixels: int,
        max_image_side: int,
        max_total_bytes: int,
    ) -> None:
        self.root = Path(os.path.abspath(os.fspath(root)))
        self.max_image_bytes = self._positive_limit("max_image_bytes", max_image_bytes)
        self.max_image_pixels = self._positive_limit("max_image_pixels", max_image_pixels)
        self.max_image_side = self._positive_limit("max_image_side", max_image_side)
        self.max_total_bytes = self._positive_limit("max_total_bytes", max_total_bytes)
        self._lock = threading.RLock()

        created = False
        try:
            self.root.mkdir(mode=0o700)
            created = True
        except FileExistsError:
            pass
        except FileNotFoundError as exc:
            raise FileNotFoundError("attachment root parent directory does not exist") from exc

        if created:
            root_stat = os.lstat(self.root)
            if not stat.S_ISDIR(root_stat.st_mode):
                raise NotADirectoryError(f"attachment root is not a directory: {self.root}")
            if root_stat.st_uid != os.getuid():
                raise PermissionError("attachment root must be owned by the current user")
            os.chmod(self.root, 0o700)

        root_fd = self._open_root()
        os.close(root_fd)

    @staticmethod
    def _positive_limit(name: str, value: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{name} must be a positive integer")
        return value

    @staticmethod
    def _validate_token(token: str) -> None:
        if not isinstance(token, str) or _TOKEN_PATTERN.fullmatch(token) is None:
            raise ValueError("invalid attachment token")

    @staticmethod
    def _validate_expected_size(expected_size: int) -> None:
        if isinstance(expected_size, bool) or not isinstance(expected_size, int):
            raise ValueError("expected_size must be a non-negative integer")
        if expected_size < 0:
            raise ValueError("expected_size must be a non-negative integer")

    @staticmethod
    def _validate_expected_sha256(expected_sha256: str) -> None:
        if (
            not isinstance(expected_sha256, str)
            or _SHA256_PATTERN.fullmatch(expected_sha256) is None
        ):
            raise ValueError("expected_sha256 must be 64 lowercase hexadecimal characters")

    @staticmethod
    def _validate_root_stat(root_stat: os.stat_result) -> None:
        if not stat.S_ISDIR(root_stat.st_mode):
            raise NotADirectoryError("attachment root is not a directory")
        if root_stat.st_uid != os.getuid():
            raise PermissionError("attachment root must be owned by the current user")
        if stat.S_IMODE(root_stat.st_mode) != 0o700:
            raise PermissionError("attachment root permissions must be 0700")

    def _open_root(self) -> int:
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
        try:
            root_fd = os.open(self.root, flags)
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise ValueError("attachment root must not be a symlink") from exc
            raise
        try:
            self._validate_root_stat(os.fstat(root_fd))
        except BaseException:
            os.close(root_fd)
            raise
        return root_fd

    @contextmanager
    def _locked_root(self, *, exclusive: bool) -> Iterator[int]:
        with self._lock:
            root_fd = self._open_root()
            try:
                operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
                fcntl.flock(root_fd, operation)
                self._validate_root_stat(os.fstat(root_fd))
                yield root_fd
            finally:
                os.close(root_fd)

    def _inspect_image(self, data: bytes) -> tuple[str, int, int]:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("error", Image.DecompressionBombWarning)
                with Image.open(io.BytesIO(data)) as image:
                    image_format = image.format
                    width, height = image.size
                    self._validate_dimensions(width, height)
                    if image_format not in _MIME_TYPES:
                        raise ValueError("unsupported image format")
                    image.verify()

                with Image.open(io.BytesIO(data)) as image:
                    if image.format != image_format or image.size != (width, height):
                        raise ValueError("image metadata changed while decoding")
                    self._validate_dimensions(*image.size)
                    frame_count = getattr(image, "n_frames", 1)
                    if frame_count > 1:
                        raise ValueError("animated images are not supported")
                    image.load()
        except Exception as exc:
            raise ValueError("invalid or unsupported image data") from exc

        return _MIME_TYPES[image_format], width, height

    def _validate_dimensions(self, width: int, height: int) -> None:
        if width <= 0 or height <= 0:
            raise ValueError("image dimensions must be positive")
        if width > self.max_image_side or height > self.max_image_side:
            raise ValueError("image side exceeds configured limit")
        if width * height > self.max_image_pixels:
            raise ValueError("image pixel count exceeds configured limit")

    @staticmethod
    def _entry_exists(root_fd: int, name: str) -> bool:
        try:
            os.stat(name, dir_fd=root_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        return True

    @staticmethod
    def _stored_bytes(root_fd: int) -> int:
        total = 0
        for name in os.listdir(root_fd):
            if _TOKEN_PATTERN.fullmatch(name) is None:
                continue
            try:
                entry_stat = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
            except FileNotFoundError:
                continue
            if stat.S_ISREG(entry_stat.st_mode):
                total += entry_stat.st_size
        return total

    @staticmethod
    def _create_temporary(root_fd: int) -> tuple[str, int]:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW
        for _ in range(128):
            name = f".tmp-{secrets.token_hex(16)}"
            try:
                temporary_fd = os.open(name, flags, 0o600, dir_fd=root_fd)
            except FileExistsError:
                continue
            try:
                os.fchmod(temporary_fd, 0o600)
            except BaseException:
                os.close(temporary_fd)
                with suppress(FileNotFoundError):
                    os.unlink(name, dir_fd=root_fd)
                raise
            return name, temporary_fd
        raise FileExistsError("could not allocate a unique attachment temporary file")

    @staticmethod
    def _write_all(file_fd: int, data: bytes) -> None:
        remaining = memoryview(data)
        while remaining:
            try:
                written = os.write(file_fd, remaining)
            except InterruptedError:
                continue
            if written == 0:
                raise OSError(errno.EIO, "short write while saving attachment")
            remaining = remaining[written:]

    def save(self, token: str, data: bytes) -> ImageMetadata:
        self._validate_token(token)
        if not isinstance(data, bytes):
            raise TypeError("attachment data must be bytes")
        byte_size = len(data)
        if byte_size == 0:
            raise ValueError("attachment data must not be empty")
        if byte_size > self.max_image_bytes:
            raise ValueError("image exceeds configured byte limit")

        mime_type, width, height = self._inspect_image(data)
        digest = hashlib.sha256(data).hexdigest()
        metadata = ImageMetadata(token, mime_type, byte_size, digest, width, height)

        with self._locked_root(exclusive=True) as root_fd:
            if self._entry_exists(root_fd, token):
                raise FileExistsError(errno.EEXIST, "attachment already exists", token)
            if self._stored_bytes(root_fd) > self.max_total_bytes - byte_size:
                raise ValueError("aggregate attachment quota exceeded")

            temporary_name: str | None = None
            temporary_fd: int | None = None
            try:
                temporary_name, temporary_fd = self._create_temporary(root_fd)
                self._write_all(temporary_fd, data)
                os.fsync(temporary_fd)
                os.close(temporary_fd)
                temporary_fd = None

                os.link(
                    temporary_name,
                    token,
                    src_dir_fd=root_fd,
                    dst_dir_fd=root_fd,
                    follow_symlinks=False,
                )
                os.unlink(temporary_name, dir_fd=root_fd)
                temporary_name = None
                os.fsync(root_fd)
            finally:
                if temporary_fd is not None:
                    os.close(temporary_fd)
                if temporary_name is not None:
                    with suppress(FileNotFoundError):
                        os.unlink(temporary_name, dir_fd=root_fd)

        return metadata

    @staticmethod
    def _validate_attachment_stat(file_stat: os.stat_result) -> None:
        if not stat.S_ISREG(file_stat.st_mode):
            raise ValueError("attachment is not a regular file")
        if file_stat.st_uid != os.getuid():
            raise PermissionError("attachment must be owned by the current user")
        mode = stat.S_IMODE(file_stat.st_mode)
        if mode & 0o7077:
            raise PermissionError("attachment must not grant group or other access")

    @staticmethod
    def _read_exact(file_fd: int, expected_size: int) -> bytes:
        result = bytearray()
        while len(result) <= expected_size:
            amount = min(_READ_CHUNK_SIZE, expected_size + 1 - len(result))
            try:
                chunk = os.read(file_fd, amount)
            except InterruptedError:
                continue
            if not chunk:
                break
            result.extend(chunk)
        return bytes(result)

    def read(self, token: str, *, expected_size: int, expected_sha256: str) -> bytes:
        self._validate_token(token)
        self._validate_expected_size(expected_size)
        self._validate_expected_sha256(expected_sha256)
        if expected_size > self.max_image_bytes:
            raise ValueError("expected_size exceeds configured byte limit")

        with self._locked_root(exclusive=False) as root_fd:
            flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
            file_fd = os.open(token, flags, dir_fd=root_fd)
            try:
                before = os.fstat(file_fd)
                self._validate_attachment_stat(before)
                if before.st_size != expected_size:
                    raise ValueError("attachment size mismatch")

                data = self._read_exact(file_fd, expected_size)
                after = os.fstat(file_fd)
                self._validate_attachment_stat(after)
                if (
                    after.st_dev != before.st_dev
                    or after.st_ino != before.st_ino
                    or after.st_size != expected_size
                    or len(data) != expected_size
                ):
                    raise ValueError("attachment size changed while reading")
            finally:
                os.close(file_fd)

        actual_sha256 = hashlib.sha256(data).hexdigest()
        if not hmac.compare_digest(actual_sha256, expected_sha256):
            raise ValueError("attachment SHA-256 mismatch")
        return data

    def delete(self, token: str) -> bool:
        self._validate_token(token)
        with self._locked_root(exclusive=True) as root_fd:
            try:
                entry_stat = os.stat(token, dir_fd=root_fd, follow_symlinks=False)
            except FileNotFoundError:
                return False
            self._validate_attachment_stat(entry_stat)
            os.unlink(token, dir_fd=root_fd)
            os.fsync(root_fd)
            return True

    def cleanup_temporary_files(self) -> int:
        removed = 0
        with self._locked_root(exclusive=True) as root_fd:
            for name in os.listdir(root_fd):
                if _TEMPORARY_PATTERN.fullmatch(name) is None:
                    continue
                try:
                    os.unlink(name, dir_fd=root_fd)
                except (FileNotFoundError, IsADirectoryError):
                    continue
                removed += 1
            if removed:
                os.fsync(root_fd)
        return removed
