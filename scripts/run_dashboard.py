#!/usr/bin/env python
"""Launch the dashboard app without requiring installation."""

from __future__ import annotations

import sys
from pathlib import Path

import uvicorn


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    src_path = repo_root / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    uvicorn.run(
        "dashboard.app:create_app",
        host="0.0.0.0",
        port=8000,
        factory=True,
    )


if __name__ == "__main__":
    main()
