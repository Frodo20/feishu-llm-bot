#!/usr/bin/env python3
"""Build an allowlisted source release without local credentials or runtime data."""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import tarfile
import tomllib
from pathlib import Path


def build(project, output):
    files = {}
    for folder, pattern in (("src/feishu_llm_bot", "*.py"), ("tests", "*.py"),
                            ("scripts", "*.py"), ("node-channel", "*.mjs")):
        for path in sorted((project / folder).glob(pattern)):
            if path.is_symlink():
                raise ValueError(f"Release input is a symlink: {path.name}")
            files[str(path.relative_to(project))] = path.read_bytes()
    for name in ("pyproject.toml", "node-channel/package.json", "node-channel/package-lock.json",
                 "PORTABLE_DEPLOYMENT.md", "PORTABLE_ARCHITECTURE.md"):
        files[name] = (project / name).read_bytes()
    files["README.md"] = files["PORTABLE_DEPLOYMENT.md"]
    files["TECHNICAL_DESIGN.md"] = files["PORTABLE_ARCHITECTURE.md"]
    files[".env.test"] = b"# Deliberately empty environment for fake bridge tests.\n"
    files["AGENTS.md"] = (
        b"# Project development\n\nRead TECHNICAL_DESIGN.md before changing the runtime. "
        b"Keep the design and validation record current with code changes. "
        b"Preserve attempt authentication, one execution slot, separate delivery, and "
        b"reconciliation before replaying uncertain writes. Use isolated test state; "
        b"do not start a second receiver or copy credentials into the repository.\n"
    )
    manifest = {
        "version": tomllib.loads(files["pyproject.toml"].decode())["project"]["version"],
        "files": {
            name: hashlib.sha256(content).hexdigest() for name, content in sorted(files.items())
        },
    }
    files["MANIFEST.json"] = json.dumps(manifest, indent=2).encode() + b"\n"
    output.parent.mkdir(parents=True, exist_ok=True)
    # A release is immutable. Never silently replace an already handed-out archive.
    with output.open("xb") as stream, tarfile.open(fileobj=stream, mode="w:gz") as archive:
        for name, content in sorted(files.items()):
            item = tarfile.TarInfo("feishu-llm-bot/" + name)
            item.size, item.mode, item.mtime = len(content), 0o644, 0
            archive.addfile(item, io.BytesIO(content))
    return {"archive": str(output), "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
            "files": len(files)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(build(Path(__file__).resolve().parents[1], args.output), indent=2))


if __name__ == "__main__":
    main()
