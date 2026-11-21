"""Durable per-chat cursors (last handled ``sortKey``) under ``HERMES_HOME/beeper/``.

The Desktop API WebSocket has no replay and ``seq`` is per connection, so after a restart or
reconnect the adapter backfills each allowlisted chat from its cursor over REST. Without a saved
cursor it starts from "now" (the newest message), so a restart never answers history.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)

_SUBDIR = "beeper"
_FILENAME = "chat-cursors.json"
_VERSION = 1


def cursor_path() -> Path:
    from hermes_constants import get_hermes_home
    return get_hermes_home() / _SUBDIR / _FILENAME


def load_cursors(base_url: str, path: Path | None = None) -> dict[str, str]:
    """Saved cursors for ``base_url``; another installation's file is ignored (local IDs collide)."""
    path = path or cursor_path()
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except FileNotFoundError:
        return {}
    except (OSError, ValueError):
        logger.warning("Beeper: chat cursor file is unreadable, starting from the newest messages", exc_info=True)
        return {}
    if not isinstance(data, dict) or data.get("version") != _VERSION or data.get("base_url") != base_url:
        return {}
    chats = data.get("chats")
    if not isinstance(chats, dict):
        return {}
    return {str(k): str(v) for k, v in chats.items() if isinstance(v, (str, int)) and str(v)}


def save_cursors(base_url: str, cursors: dict[str, str], path: Path | None = None) -> None:
    """Atomic owner-only write: temp file (0600) in the same directory, then ``os.replace``."""
    path = path or cursor_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps({"version": _VERSION, "base_url": base_url, "chats": dict(sorted(cursors.items()))}, indent=2)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.stem}_", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise
