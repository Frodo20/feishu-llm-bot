#!/usr/bin/env python3
"""Install this source checkout in its own venv; never starts a bot or installs global hooks."""

from __future__ import annotations

import argparse
import importlib.util
import json
import platform
import shutil
import subprocess
import sys
from pathlib import Path


def install(project, *, development=False):
    if sys.version_info < (3, 11):  # noqa: UP036 -- installer runs before package requirements
        raise ValueError("Run this installer with Python 3.11 or newer")
    if platform.system() not in {"Linux", "Darwin"}:
        raise ValueError("Use Linux, macOS, or WSL2; native Windows is not supported")
    for binary in ("node", "npm"):
        if not shutil.which(binary):
            raise ValueError(f"Install Node.js 20+ with npm first: {binary} is missing from PATH")
    version = subprocess.check_output(["node", "--version"], text=True, timeout=10).strip()
    if int(version.lstrip("v").split(".")[0]) < 20:
        raise ValueError("Node.js 20 or newer is required")
    venv = project / ".venv"
    if venv.is_symlink():
        raise ValueError("Refusing to install into a shared/symlinked venv; use a fresh checkout")
    python = venv / "bin/python"
    ready = python.exists() and subprocess.run(
        [str(python), "-m", "pip", "--version"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10,
    ).returncode == 0
    if not ready:
        if importlib.util.find_spec("ensurepip") is not None:
            subprocess.run([sys.executable, "-m", "venv", str(venv)], check=True)
        elif shutil.which("virtualenv"):
            subprocess.run(["virtualenv", "--no-periodic-update", "--python", sys.executable,
                            str(venv)], check=True)
        else:
            raise ValueError("Install python3-venv (Debian/Ubuntu) or virtualenv, then rerun")
    target = str(project) + ("[test]" if development else "")
    subprocess.run([str(python), "-m", "pip", "install", "-e", target], check=True)
    subprocess.run(["npm", "ci", "--prefix", str(project / "node-channel")], check=True)
    print(json.dumps({"installed": str(project), "command": str(venv / "bin/feishu-bot"),
                      "next": "feishu-bot init --help"}, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dev", action="store_true", help="Also install test/lint dependencies")
    args = parser.parse_args()
    try:
        install(Path(__file__).resolve().parents[1], development=args.dev)
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        parser.exit(1, f"install: {exc}\n")


if __name__ == "__main__":
    main()
