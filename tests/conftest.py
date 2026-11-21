"""Redacted Beeper Desktop API captures (Beeper Desktop 4.3.22).

``ws_session.jsonl`` is one real WebSocket session subscribed to every chat, with IDs remapped,
texts replaced by ``owner text <id>`` / ``friend text <id>`` and chat objects dropped.
``rest_messages_page.json`` is a redacted ``GET /v1/chats/{id}/messages`` page.
"""

import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest

# The plugin runs inside Hermes: put a hermes-agent checkout on the path (HERMES_AGENT_REPO, default
# ~/.hermes/hermes-agent), then load ../beeper as the package ``beeper_platform``.
_HERMES = Path(os.environ.get("HERMES_AGENT_REPO", Path.home() / ".hermes" / "hermes-agent")).expanduser()
if not (_HERMES / "gateway" / "platforms" / "base.py").exists():
    raise pytest.UsageError(f"hermes-agent checkout not found at {_HERMES}; set HERMES_AGENT_REPO")
sys.path.insert(0, str(_HERMES))
sys.path.insert(0, str(Path(__file__).resolve().parent))  # tests/_waiting.py
_PLUGIN = Path(__file__).resolve().parent.parent  # the repo root is the plugin
_spec = importlib.util.spec_from_file_location("beeper_platform", _PLUGIN / "__init__.py",
                                               submodule_search_locations=[str(_PLUGIN)])
assert _spec is not None and _spec.loader is not None
sys.modules["beeper_platform"] = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sys.modules["beeper_platform"])

from beeper_platform.events import BeeperMessage, Frame, parse_frame  # noqa: E402


def _register_platform() -> None:
    """Register the plugin the way Hermes does on load, so ``Platform("beeper")`` resolves."""
    from gateway.platform_registry import PlatformEntry, platform_registry

    class _Ctx:
        def register_platform(self, **kwargs):
            platform_registry.register(PlatformEntry(**kwargs))

    if not platform_registry.is_registered("beeper"):
        sys.modules["beeper_platform"].register(_Ctx())


_register_platform()

_DIR = Path(__file__).parent / "fixtures"
OWNER = "@owner:beeper.com"


@pytest.fixture(autouse=True)
def _hermes_home(tmp_path, monkeypatch):
    """Every test gets its own HERMES_HOME (cursors, dry-run file, pairing store)."""
    home = tmp_path / "hermes-home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))


@pytest.fixture
def owner_id() -> str:
    return OWNER


@pytest.fixture(scope="session")
def ws_frames() -> list[Frame]:
    return [parse_frame(line) for line in (_DIR / "ws_session.jsonl").read_text(encoding="utf-8").splitlines() if line]


@pytest.fixture
def rest_page() -> dict:
    return json.loads((_DIR / "rest_messages_page.json").read_text(encoding="utf-8"))


def _message(**overrides) -> BeeperMessage:
    raw = {"id": "1000", "chatID": "!chat1:beeper.local", "senderID": OWNER, "senderName": "Owner",
           "isSender": True, "text": "!agent hello", "type": "TEXT", "timestamp": "2026-01-01T12:00:00Z",
           "sortKey": "5000"}
    raw.update(overrides)
    msg = BeeperMessage.from_api(raw)
    assert msg is not None
    return msg


@pytest.fixture
def make_message():
    """Factory for a ``BeeperMessage``: an owner ``!agent hello`` in ``!chat1`` unless overridden."""
    return _message
