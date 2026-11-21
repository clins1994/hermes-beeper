"""Beeper settings, parsed once from ``gateway.platforms.beeper`` (``PlatformConfig.extra``).

Everything except the access token lives in ``config.yaml``: the per-chat allowlist is a nested map
that env vars cannot express. The token is a secret and comes only from ``BEEPER_ACCESS_TOKEN``.
Invalid settings raise :class:`BeeperConfigError` with the key and the fix, so a typo fails the
gateway start instead of silently widening or narrowing who the agent answers.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

DEFAULT_BASE_URL = "http://127.0.0.1:23373"
DEFAULT_TRIGGER = "!hermes"
DEFAULT_REPLY_MARKER = "🤖 "
SELF_INVOKER = "self"
ANY_INVOKER = "*"

ChatMode = Literal["prefix", "always"]
_MODES: tuple[ChatMode, ...] = ("prefix", "always")


class BeeperConfigError(ValueError):
    """A ``gateway.platforms.beeper`` setting is invalid; the message names the key and the fix."""


@dataclass(frozen=True)
class ChatRule:
    """One allowlisted chat. ``chat_key`` is what the user wrote (chat ID or local chat ID)."""

    chat_key: str
    label: str
    mode: ChatMode
    trigger: str
    invokers: frozenset[str]
    toolsets: tuple[str, ...] | None
    processing_reaction: str | None = None  # None: inherit the platform-wide setting

    def allows_sender(self, *, sender_id: str, is_self: bool) -> bool:
        if ANY_INVOKER in self.invokers:
            return True
        if is_self:
            return SELF_INVOKER in self.invokers
        return sender_id in self.invokers


@dataclass(frozen=True)
class RateLimits:
    per_chat_seconds: float = 2.0
    global_per_minute: int = 20
    breaker_replies: int = 10
    breaker_window_seconds: float = 600.0


@dataclass(frozen=True)
class BeeperSettings:
    base_url: str
    dry_run: bool
    trigger: str
    reply_marker: str
    processing_reaction: str
    max_backfill_age_seconds: float
    rate_limits: RateLimits
    chats: tuple[ChatRule, ...]


def _str(extra: Mapping[str, Any], key: str, default: str) -> str:
    value = extra.get(key, default)
    if value is None:
        return default
    if not isinstance(value, str):
        raise BeeperConfigError(f"gateway.platforms.beeper.{key} must be a string, got {type(value).__name__}")
    return value


def _bool(value: Any, key: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in {"true", "false", "yes", "no", "1", "0"}:
        return value.strip().lower() in {"true", "yes", "1"}
    raise BeeperConfigError(f"gateway.platforms.beeper.{key} must be true or false, got {value!r}")


def _positive(value: Any, key: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise BeeperConfigError(f"gateway.platforms.beeper.{key} must be a number, got {value!r}") from None
    if number <= 0:
        raise BeeperConfigError(f"gateway.platforms.beeper.{key} must be greater than 0, got {value!r}")
    return number


def _trigger(value: str, key: str) -> str:
    trigger = value.strip()
    if not trigger or any(ch.isspace() for ch in trigger):
        raise BeeperConfigError(f"gateway.platforms.beeper.{key} must be one word with no spaces, got {value!r}")
    return trigger


def _reaction(value: Any, key: str) -> str:
    """Reaction shown on the triggering message while the agent works; "" (or null/false) = off."""
    if value is None or value is False:
        return ""
    reaction = value.strip() if isinstance(value, str) else None
    if reaction is None or any(ch.isspace() for ch in reaction):
        raise BeeperConfigError(f"gateway.platforms.beeper.{key} must be one emoji or empty, got {value!r}")
    return reaction


def _rate_limits(raw: Any) -> RateLimits:
    if raw is None:
        return RateLimits()
    if not isinstance(raw, Mapping):
        raise BeeperConfigError("gateway.platforms.beeper.rate_limit must be a mapping")
    defaults = RateLimits()
    return RateLimits(
        per_chat_seconds=_positive(raw.get("per_chat_seconds", defaults.per_chat_seconds), "rate_limit.per_chat_seconds"),
        global_per_minute=int(_positive(raw.get("global_per_minute", defaults.global_per_minute),
                                        "rate_limit.global_per_minute")),
        breaker_replies=int(_positive(raw.get("breaker_replies", defaults.breaker_replies), "rate_limit.breaker_replies")),
        breaker_window_seconds=_positive(raw.get("breaker_window_seconds", defaults.breaker_window_seconds),
                                         "rate_limit.breaker_window_seconds"),
    )


def _invokers(raw: Any, key: str) -> frozenset[str]:
    if raw is None:
        return frozenset({SELF_INVOKER})
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, list) or not all(isinstance(item, str) and item.strip() for item in raw):
        raise BeeperConfigError(f"{key} must be a list of sender IDs, 'self' or '*'")
    return frozenset(item.strip() for item in raw)


def _toolsets(raw: Any, key: str) -> tuple[str, ...] | None:
    if raw is None:
        return None
    if not isinstance(raw, list) or not all(isinstance(item, str) and item.strip() for item in raw):
        raise BeeperConfigError(f"{key} must be a list of toolset names")
    return tuple(item.strip() for item in raw)


def _chat_rule(chat_key: Any, raw: Any, default_trigger: str) -> ChatRule:
    key = f"gateway.platforms.beeper.chats[{chat_key!r}]"
    if not isinstance(chat_key, (str, int)) or not str(chat_key).strip():
        raise BeeperConfigError(f"{key}: chat keys must be a Beeper chat ID or local chat ID")
    raw = {} if raw is None else raw
    if not isinstance(raw, Mapping):
        raise BeeperConfigError(f"{key} must be a mapping (label, mode, trigger, invokers, toolsets)")
    mode = raw.get("mode", "prefix")
    if mode not in _MODES:
        raise BeeperConfigError(f"{key}.mode must be one of {', '.join(_MODES)}, got {mode!r}")
    trigger = raw.get("trigger")
    return ChatRule(
        chat_key=str(chat_key).strip(),
        label=str(raw.get("label") or chat_key),
        mode=mode,
        trigger=_trigger(trigger, f"chats[{chat_key!r}].trigger") if trigger is not None else default_trigger,
        invokers=_invokers(raw.get("invokers"), f"{key}.invokers"),
        toolsets=_toolsets(raw.get("toolsets"), f"{key}.toolsets"),
        processing_reaction=_reaction(raw["processing_reaction"], f"chats[{chat_key!r}].processing_reaction")
        if "processing_reaction" in raw else None,
    )


def parse_settings(extra: Mapping[str, Any]) -> BeeperSettings:
    """Parse and validate ``PlatformConfig.extra``. An absent ``chats`` map means no chat is active."""
    trigger = _trigger(_str(extra, "trigger", DEFAULT_TRIGGER), "trigger")
    marker = _str(extra, "reply_marker", DEFAULT_REPLY_MARKER)
    if not marker.strip():
        raise BeeperConfigError(
            "gateway.platforms.beeper.reply_marker cannot be empty: it is how friends tell the agent from you "
            "and how the adapter recognises its own replies")
    raw_chats = extra.get("chats") or {}
    if not isinstance(raw_chats, Mapping):
        raise BeeperConfigError("gateway.platforms.beeper.chats must be a mapping of chat ID -> settings")
    return BeeperSettings(
        base_url=_str(extra, "base_url", DEFAULT_BASE_URL).rstrip("/") or DEFAULT_BASE_URL,
        dry_run=_bool(extra.get("dry_run", False), "dry_run"),
        trigger=trigger,
        reply_marker=marker,
        processing_reaction=_reaction(extra.get("processing_reaction"), "processing_reaction"),
        max_backfill_age_seconds=_positive(extra.get("max_backfill_age_seconds", 600), "max_backfill_age_seconds"),
        rate_limits=_rate_limits(extra.get("rate_limit")),
        chats=tuple(_chat_rule(k, v, trigger) for k, v in raw_chats.items()),
    )
