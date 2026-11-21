"""Beeper settings parsing: deny-by-default and loud failures on settings that change who is answered."""

import pytest
from beeper_platform.config import (
    ANY_INVOKER,
    DEFAULT_BASE_URL,
    DEFAULT_REPLY_MARKER,
    DEFAULT_TRIGGER,
    SELF_INVOKER,
    BeeperConfigError,
    parse_settings,
)


def test_empty_config_activates_no_chat():
    settings = parse_settings({})
    assert settings.chats == ()
    assert settings.base_url == DEFAULT_BASE_URL
    assert settings.trigger == DEFAULT_TRIGGER
    assert settings.reply_marker == DEFAULT_REPLY_MARKER
    assert settings.dry_run is False


def test_chat_defaults_to_prefix_mode_and_owner_only():
    settings = parse_settings({"trigger": "!agent", "chats": {"101": {"label": "notes"}}})
    (rule,) = settings.chats
    assert rule.chat_key == "101"
    assert rule.mode == "prefix"
    assert rule.trigger == "!agent"
    assert rule.invokers == frozenset({SELF_INVOKER})
    assert rule.allows_sender(sender_id="@owner:beeper.com", is_self=True)
    assert not rule.allows_sender(sender_id="@friend1:beeper.local", is_self=False)


def test_chat_overrides_trigger_invokers_and_toolsets():
    settings = parse_settings({"chats": {"!a:beeper.local": {
        "mode": "always", "trigger": "@h", "invokers": ["self", "@friend1:beeper.local"], "toolsets": ["web"]}}})
    (rule,) = settings.chats
    assert (rule.mode, rule.trigger, rule.toolsets) == ("always", "@h", ("web",))
    assert rule.allows_sender(sender_id="@friend1:beeper.local", is_self=False)
    assert not rule.allows_sender(sender_id="@friend2:beeper.local", is_self=False)


def test_wildcard_invoker_admits_everyone_in_that_chat():
    (rule,) = parse_settings({"chats": {"x": {"invokers": ANY_INVOKER}}}).chats
    assert rule.allows_sender(sender_id="@anyone:beeper.local", is_self=False)


@pytest.mark.parametrize("extra, fragment", [
    ({"reply_marker": "  "}, "reply_marker cannot be empty"),
    ({"trigger": "two words"}, "trigger must be one word"),
    ({"chats": {"x": {"mode": "sometimes"}}}, "mode must be one of"),
    ({"chats": {"x": {"invokers": [""]}}}, "invokers must be a list"),
    ({"chats": ["x"]}, "chats must be a mapping"),
    ({"dry_run": "maybe"}, "dry_run must be true or false"),
    ({"rate_limit": {"per_chat_seconds": 0}}, "per_chat_seconds must be greater than 0"),
])
def test_invalid_settings_fail_loudly(extra, fragment):
    with pytest.raises(BeeperConfigError, match=fragment):
        parse_settings(extra)
