"""Beeper authorization env names (``PlatformEntry`` fields), readable by core without importing the plugin.

The adapter gates every chat itself (``gateway.platforms.beeper.chats``); these optional env
allowlists only narrow that further. ``allowlist_suppresses_pairing`` keeps the gateway from ever
answering an unknown sender with a pairing code: on Beeper that code would go out from the
owner's own account.
"""

AUTHZ = {
    "allowed_users_env": "BEEPER_ALLOWED_USERS",
    "allow_all_env": "BEEPER_ALLOW_ALL_USERS",
    "allowlist_suppresses_pairing": True,
}
