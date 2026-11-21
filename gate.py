"""Intake gate: decide whether one Beeper message wakes the agent.

The agent posts from the owner's own account, so the owner's messages, the agent's replies and
friends' messages all arrive as ordinary ``message.upserted`` events. Layers, in order:

1. echo: a message ID the adapter sent (``pendingMessageID``) is never handled;
2. marker: an own message that starts with the reply marker is the agent's, not the owner's;
3. once: a message ID is handled at most once (upserts repeat for pending/sent/seen/edits);
4. cursor: a message at or before the chat's cursor is old (an upsert of a read or edited message);
5. type: reactions, notices and unknown types never wake the agent;
6. invoker and trigger: the chat rule decides who may invoke it and whether a prefix is required;
7. breaker: a chat that hit the reply ceiling is paused.
"""

from __future__ import annotations

import time
from collections import OrderedDict, deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

from .config import ChatRule, RateLimits
from .events import MEDIA_TYPES, TEXT_TYPE, BeeperMessage, sort_key_after


class Drop(Enum):
    ECHO = "echo"
    MARKER = "marker"
    DUPLICATE = "duplicate"
    OLD = "old"
    TYPE = "type"
    INVOKER = "invoker"
    NO_TRIGGER = "no_trigger"
    EMPTY = "empty"
    BREAKER = "breaker"


@dataclass(frozen=True)
class Admit:
    text: str


class BoundedIdSet:
    """Insertion-ordered set capped at ``cap`` entries (oldest evicted)."""

    def __init__(self, cap: int) -> None:
        self._cap = cap
        self._ids: OrderedDict[str, None] = OrderedDict()

    def add(self, item: str) -> None:
        self._ids[item] = None
        self._ids.move_to_end(item)
        while len(self._ids) > self._cap:
            self._ids.popitem(last=False)

    def __contains__(self, item: object) -> bool:
        return item in self._ids


def strip_trigger(text: str, trigger: str) -> str | None:
    """Text after ``trigger`` when the message starts with it as a whole word (case-insensitive), else None."""
    stripped = text.lstrip()
    if not stripped[: len(trigger)].lower() == trigger.lower():
        return None
    rest = stripped[len(trigger):]
    if rest and rest[0].isalnum():
        return None  # "!agentx" is not "!agent"
    return rest.lstrip(" \t,:;-").strip()


def _content(msg: BeeperMessage) -> str | None:
    """Text the agent sees, with a placeholder for media; None for types that never wake it."""
    if msg.type == TEXT_TYPE:
        return msg.text
    if msg.type in MEDIA_TYPES:
        placeholder = f"[attachment: {msg.type.lower()}]"
        return f"{placeholder} {msg.text}".strip() if msg.text else placeholder
    return None


class ReplyBreaker:
    """Per-chat ceiling on agent replies in a sliding window (loop circuit breaker)."""

    def __init__(self, limits: RateLimits, clock: Callable[[], float] = time.monotonic) -> None:
        self._limit = limits.breaker_replies
        self._window = limits.breaker_window_seconds
        self._clock = clock
        self._sent: dict[str, deque[float]] = {}

    def _recent(self, chat_id: str) -> deque[float]:
        stamps = self._sent.setdefault(chat_id, deque())
        cutoff = self._clock() - self._window
        while stamps and stamps[0] <= cutoff:
            stamps.popleft()
        return stamps

    def tripped(self, chat_id: str) -> bool:
        return len(self._recent(chat_id)) >= self._limit

    def record(self, chat_id: str) -> None:
        self._recent(chat_id).append(self._clock())


class IntakeGate:
    """Stateful gate: sent IDs, handled IDs and per-chat cursors. One per adapter."""

    def __init__(self, *, reply_marker: str, breaker: ReplyBreaker, cursors: dict[str, str] | None = None) -> None:
        self.reply_marker = reply_marker
        self.breaker = breaker
        self.sent_ids = BoundedIdSet(2048)
        self.handled_ids = BoundedIdSet(8192)
        self.cursors: dict[str, str] = dict(cursors or {})

    def note_sent(self, message_id: str) -> None:
        if message_id:
            self.sent_ids.add(message_id)

    def advance(self, msg: BeeperMessage) -> bool:
        """Move the chat cursor past ``msg``; False when ``msg`` is not newer than the cursor."""
        if not msg.sort_key:
            return True
        if not sort_key_after(msg.sort_key, self.cursors.get(msg.chat_id)):
            return False
        self.cursors[msg.chat_id] = msg.sort_key
        return True

    def _loop_drop(self, msg: BeeperMessage) -> Drop | None:
        if msg.id in self.sent_ids:
            return Drop.ECHO
        if msg.is_sender and msg.text.startswith(self.reply_marker.strip()):
            return Drop.MARKER
        if msg.id in self.handled_ids:
            return Drop.DUPLICATE
        if not self.advance(msg):
            return Drop.OLD
        return None

    def evaluate(self, msg: BeeperMessage, rule: ChatRule) -> Admit | Drop:
        if (loop := self._loop_drop(msg)) is not None:
            return loop
        self.handled_ids.add(msg.id)
        content = _content(msg)
        if content is None:
            return Drop.TYPE
        if not rule.allows_sender(sender_id=msg.sender_id, is_self=msg.is_sender):
            return Drop.INVOKER
        text = strip_trigger(content, rule.trigger)
        if text is None:
            if rule.mode != "always":
                return Drop.NO_TRIGGER
            text = content.strip()
        if not text:
            return Drop.EMPTY
        if self.breaker.tripped(msg.chat_id):
            return Drop.BREAKER
        return Admit(text=text)
