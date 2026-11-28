"""Typed views of Beeper Desktop API WebSocket frames and message objects.

Parsed once where they enter (``parse_frame`` / ``BeeperMessage.from_api``); the rest of the plugin
never touches raw dicts. Shapes follow ``GET /v1/spec`` (``WSOutgoingMessage``, ``Message``) and were
checked against Beeper Desktop 4.3.22.
"""

from __future__ import annotations

import html
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

FrameType = Literal[
    "ready", "subscriptions.updated", "error", "chat.upserted", "chat.deleted",
    "message.upserted", "message.deleted", "app.state.updated", "unknown",
]
_FRAME_TYPES: frozenset[str] = frozenset(FrameType.__args__)  # type: ignore[attr-defined]

# Message ``type`` values that carry user content. REACTION / NOTICE / unknown types never wake the agent.
TEXT_TYPE = "TEXT"
MEDIA_TYPES: frozenset[str] = frozenset({"IMAGE", "VIDEO", "AUDIO", "VOICE", "FILE", "STICKER"})


# Formatted messages arrive with ``text`` as HTML (for example ``<p>🦥 hi</p>``), including the agent's
# own replies when the network echoes them back formatted. Everything downstream (the reply-marker loop
# guard, the trigger, the agent's input) needs the plain text.
_HTML_TAG = re.compile(r"</?(?:p|br|b|i|u|s|em|strong|del|code|pre|blockquote|a|span|ul|ol|li|h[1-6])\b[^>]*>", re.I)
_BREAK = re.compile(r"<br\s*/?>|</(?:p|li|h[1-6]|blockquote|pre)>", re.I)


def plain_text(text: str) -> str:
    """``text`` without HTML formatting: line breaks kept, tags dropped, entities decoded. Plain text is unchanged."""
    if not _HTML_TAG.search(text):
        return text
    text = _BREAK.sub("\n", text)
    text = _HTML_TAG.sub("", text)
    return html.unescape(text).strip()


class FrameError(ValueError):
    """A WebSocket frame was not a JSON object."""


@dataclass(frozen=True)
class BeeperMessage:
    """One ``Message`` from ``message.upserted`` entries or ``GET /v1/chats/{id}/messages``.

    ``id`` is the installation-local message ID. It stays the same across every upsert of a
    message (pending → sent → seen) and equals the ``pendingMessageID`` a send returns, so it is
    the key for de-duplication and echo suppression. ``sort_key`` orders messages within a chat.
    """

    id: str
    chat_id: str
    sender_id: str
    sender_name: str
    is_sender: bool
    text: str
    type: str
    timestamp: datetime | None
    sort_key: str
    reply_to_id: str | None
    attachment_count: int

    @classmethod
    def from_api(cls, raw: Mapping[str, Any]) -> BeeperMessage | None:
        """None when the object lacks an ID or chat ID (nothing can be keyed or answered)."""
        msg_id, chat_id = raw.get("id"), raw.get("chatID")
        if not msg_id or not chat_id:
            return None
        attachments = raw.get("attachments")
        return cls(
            id=str(msg_id),
            chat_id=str(chat_id),
            sender_id=str(raw.get("senderID") or ""),
            sender_name=str(raw.get("senderName") or ""),
            is_sender=raw.get("isSender") is True,
            text=plain_text(str(raw.get("text") or "")),
            type=str(raw.get("type") or ""),
            timestamp=_parse_ts(raw.get("timestamp")),
            sort_key=str(raw.get("sortKey") or ""),
            reply_to_id=str(raw["linkedMessageID"]) if raw.get("linkedMessageID") else None,
            attachment_count=len(attachments) if isinstance(attachments, list) else 0,
        )


@dataclass(frozen=True)
class Frame:
    type: FrameType
    seq: int | None
    chat_id: str | None
    messages: tuple[BeeperMessage, ...]
    error_code: str | None
    app_state: str | None


def _parse_ts(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def parse_frame(raw: str | bytes) -> Frame:
    """Parse one WebSocket text frame. Unknown frame types parse as ``unknown`` (forward compatible)."""
    try:
        data = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise FrameError(f"frame is not JSON: {exc}") from None
    if not isinstance(data, dict):
        raise FrameError("frame is not a JSON object")
    frame_type = data.get("type")
    seq = data.get("seq")
    messages: tuple[BeeperMessage, ...] = ()
    if frame_type == "message.upserted" and isinstance(data.get("entries"), list):
        messages = tuple(m for e in data["entries"] if isinstance(e, Mapping) and (m := BeeperMessage.from_api(e)))
    app_state = data.get("appState")
    return Frame(
        type=frame_type if frame_type in _FRAME_TYPES else "unknown",
        seq=seq if isinstance(seq, int) and not isinstance(seq, bool) else None,
        chat_id=str(data["chatID"]) if data.get("chatID") else None,
        messages=messages,
        error_code=str(data["code"]) if frame_type == "error" and data.get("code") else None,
        app_state=str(app_state.get("state")) if isinstance(app_state, Mapping) and app_state.get("state") else None,
    )


def sort_key_after(candidate: str, cursor: str | None) -> bool:
    """True when ``candidate`` orders after ``cursor``. Sort keys are opaque strings; the observed
    values are decimal, compared numerically, with a length-then-lexical fallback otherwise."""
    if not cursor:
        return True
    if candidate.isdigit() and cursor.isdigit():
        return int(candidate) > int(cursor)
    return (len(candidate), candidate) > (len(cursor), cursor)
