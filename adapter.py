"""Beeper platform adapter: Hermes in the owner's own chats through the local Beeper Desktop API.

Inbound: the Desktop API WebSocket (``GET /v1/ws``, ``subscriptions.set`` for the allowlisted chats
only, ``message.upserted``), plus REST backfill after every (re)connect and on ``seq`` gaps — the
socket is at-most-once with no replay. Outbound: ``POST /v1/chats/{id}/messages`` as a reply to the
triggering message, prefixed with the reply marker. Photos in admitted messages are streamed from
``GET /v1/assets/serve`` into Hermes' image cache and passed to the agent as local files.

The agent speaks from the owner's own accounts, so the adapter is deny-by-default: no chat is active
until it is listed under ``gateway.platforms.beeper.chats``, and it never sends to any other chat.
See ``gate.py`` for the loop-prevention layers and ``website/docs/user-guide/messaging/beeper.md``.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from gateway.config import Platform, PlatformConfig
from gateway.platforms._shared import get_scoped_secret
from gateway.platforms.access_policy_mixin import OwnAccessPolicyMixin
from gateway.platforms.base import BasePlatformAdapter, SendResult, cache_image_from_bytes_async
from gateway.platforms.event import MessageEvent, MessageType, ProcessingOutcome
from gateway.platforms.helpers import hash_for_log

from . import cursors as cursor_store
from .client import BeeperApiError, BeeperClient, ChatInfo, SendPacer
from .config import ANY_INVOKER, SELF_INVOKER, BeeperConfigError, BeeperSettings, ChatRule, parse_settings
from .events import Attachment, BeeperMessage, Frame, FrameError, parse_frame, sort_key_after
from .gate import Admit, IntakeGate, ReplyBreaker

if TYPE_CHECKING:
    from gateway.session import SessionSource

logger = logging.getLogger(__name__)

TOKEN_ENV = "BEEPER_ACCESS_TOKEN"
MAX_TEXT_LENGTH = 4000
_BACKFILL_MAX_PAGES = 5
_RECONNECT_MAX_SECONDS = 30.0
# Cache file extension per photo MIME type; the cache sniffs the bytes, so an unknown type still works as .jpg.
_PHOTO_EXT = {"image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif", "image/webp": ".webp",
              "image/heic": ".heic", "image/heif": ".heif", "image/avif": ".avif", "image/bmp": ".bmp"}

PLATFORM_HINT = (  # facts about the channel only: tone, length and persona belong to the user's SOUL.md
    "You are replying inside the owner's personal chats through Beeper (WhatsApp, Signal, Telegram, "
    "LINE, iMessage and other networks), posting from the owner's own account. The configured reply "
    "marker is added to the start of every message you send, so do not write it yourself. Most of "
    "these networks show Markdown as raw text."
)


@dataclass(frozen=True)
class ActiveChat:
    rule: ChatRule
    info: ChatInfo


class BeeperAdapter(OwnAccessPolicyMixin, BasePlatformAdapter):
    ALLOW_ALL_ENV_PREFIX = "BEEPER"
    SUPPORTS_MESSAGE_EDITING = False
    MAX_MESSAGE_LENGTH = MAX_TEXT_LENGTH
    splits_long_messages = True
    serial_receive_loop = True

    def __init__(self, config: PlatformConfig, *, client: BeeperClient | None = None,
                 ws_connect: Any = None) -> None:
        super().__init__(config, Platform("beeper"))
        self._config_error: str | None = None
        try:
            self.settings: BeeperSettings = parse_settings(config.extra or {})
        except BeeperConfigError as exc:
            self._config_error = str(exc)
            self.settings = parse_settings({})
        self._token = str(get_scoped_secret(TOKEN_ENV, "") or "").strip()
        self._client = client
        self._ws_connect = ws_connect
        self._chats: dict[str, ActiveChat] = {}
        self.gate = IntakeGate(reply_marker=self.settings.reply_marker,
                               breaker=ReplyBreaker(self.settings.rate_limits))
        self._pacer = SendPacer(self.settings.rate_limits)
        self._sleep = asyncio.sleep
        self._ws_task: asyncio.Task | None = None
        self._last_seq: int | None = None
        self._closing = False
        self._dry_run_count = 0
        # Own-access-policy contract (access_policy_mixin): the adapter gates every chat itself.
        self._dm_policy = "allowlist"
        self._group_policy = "allowlist"
        self._allow_from: set[str] = set()
        self._group_allow_from: set[str] = set()
        self._own_id = ""
        self._reacted: dict[tuple[str, str], str] = {}  # (chat, message) -> processing reaction shown

    # --- access policy -------------------------------------------------------------------

    def _is_dm_allowed(self, sender_id: str) -> bool:
        return ANY_INVOKER in self._allow_from or sender_id in self._allow_from

    def _principals(self, chat_type: str) -> set[str]:
        """Sender IDs any allowlisted chat of ``chat_type`` admits ("self" resolved to the own ID)."""
        principals: set[str] = set()
        for active in self._chats.values():
            if (active.info.chat_type == "group") != (chat_type == "group"):
                continue
            principals |= {self._own_id if p == SELF_INVOKER else p for p in active.rule.invokers}
        return principals

    def _publish_sender_allowlists(self) -> None:
        """Mirror the per-chat invokers into ``allow_from`` / ``group_allow_from`` on this adapter's
        own config copy. The runner re-checks every admitted message against these; they are a
        superset of the per-chat gate, which still decides first."""
        self._allow_from = self._principals("dm")
        group = self._principals("group")
        extra = dict(self.config.extra or {})
        extra["allow_from"] = sorted(self._allow_from | set(extra.get("allow_from") or []))
        extra["group_allow_from"] = sorted(group | set(extra.get("group_allow_from") or []))
        # Gateway setup/operational notices ("no home channel set", ...) must not be posted into a
        # chat from the owner's account; "private" routes them to send_private_notice (logged only).
        extra.setdefault("notice_delivery", "private")
        # Only the owner runs slash commands ("<trigger> /model ..."); everyone else keeps /help and /whoami.
        extra.setdefault("allow_admin_from", [self._own_id])
        extra.setdefault("group_allow_admin_from", [self._own_id])
        self.config = dataclasses.replace(self.config, extra=extra)

    def toolsets_for_source(self, source: SessionSource) -> list[str] | None:
        active = self._chats.get(source.chat_id or "")
        return list(active.rule.toolsets) if active and active.rule.toolsets is not None else None

    # --- lifecycle -------------------------------------------------------------------------

    def _fail(self, code: str, message: str, *, retryable: bool) -> bool:
        logger.error("[beeper] %s", message)
        self._set_fatal_error(code, message, retryable=retryable)
        return False

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if self._config_error:
            return self._fail("config_invalid", self._config_error, retryable=False)
        if not self._token:
            return self._fail("token_missing", f"{TOKEN_ENV} is not set. Copy the access token from Beeper Desktop "
                              "(Settings → Developers) into the profile's .env.", retryable=False)
        if self._client is None:
            import httpx
            from gateway.platforms._http_client_limits import platform_httpx_limits
            self._client = BeeperClient(self.settings.base_url, self._token, http=httpx.AsyncClient(
                timeout=httpx.Timeout(30.0, connect=5.0), limits=platform_httpx_limits()))
        try:
            self._own_id = await self._client.whoami()
        except BeeperApiError as exc:
            if exc.unauthorized:
                return self._fail("token_rejected", f"Beeper Desktop rejected {TOKEN_ENV} (HTTP {exc.status}). "
                                  "Create a new token in Beeper Desktop → Settings → Developers.", retryable=False)
            return self._fail("unreachable", f"{exc}. Is Beeper Desktop running with its API enabled?", retryable=True)
        if not self._acquire_platform_lock("beeper", self.settings.base_url,
                                           f"Beeper Desktop API at {self.settings.base_url}"):
            return False
        try:
            await self._resolve_chats()
        except BeeperConfigError as exc:
            self._release_platform_lock()
            return self._fail("config_invalid", str(exc), retryable=False)
        except BeeperApiError as exc:
            self._release_platform_lock()
            return self._fail("unreachable", str(exc), retryable=exc.retryable)
        self._publish_sender_allowlists()
        self.gate.cursors.update(cursor_store.load_cursors(self.settings.base_url))
        self._closing = False
        self._ws_task = asyncio.create_task(self._ws_loop(), name="beeper-ws")
        self._mark_connected()
        self._wire_plugin_handlers(None)
        logger.info("[beeper] connected to %s: %d allowlisted chat(s)%s", self.settings.base_url,
                    len(self._chats), " [dry-run: nothing will be sent]" if self.settings.dry_run else "")
        return True

    async def _resolve_chats(self) -> None:
        """Map each configured chat key (chat ID or local ID) to its canonical chat ID."""
        assert self._client is not None
        chats: dict[str, ActiveChat] = {}
        for rule in self.settings.chats:
            try:
                info = await self._client.get_chat(rule.chat_key)
            except BeeperApiError as exc:
                if exc.status == 404:
                    raise BeeperConfigError(f"gateway.platforms.beeper.chats: chat {rule.chat_key!r} ({rule.label}) "
                                            "was not found in Beeper Desktop") from None
                raise
            chats[info.id] = ActiveChat(rule=rule, info=info)
        self._chats = chats
        self._group_allow_from = set(chats)

    async def disconnect(self) -> None:
        self._closing = True
        if self._ws_task is not None:
            self._ws_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._ws_task
            self._ws_task = None
        self._save_cursors()
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        self._release_platform_lock()
        self._mark_disconnected()

    # --- inbound ---------------------------------------------------------------------------

    async def _open_socket(self):
        assert self._client is not None
        if self._ws_connect is not None:
            return await self._ws_connect(self._client.ws_url, self._client.auth_headers)
        import websockets
        return await websockets.connect(self._client.ws_url, additional_headers=self._client.auth_headers,
                                        max_size=8 * 1024 * 1024, open_timeout=10)

    async def _ws_loop(self) -> None:
        delay = 1.0
        while not self._closing:
            try:
                ws = await self._open_socket()
                try:
                    await self._run_socket(ws)
                    delay = 1.0
                finally:
                    with contextlib.suppress(Exception):
                        await ws.close()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # any transport failure: degrade, back off, reconnect, backfill
                logger.warning("[beeper] WebSocket closed (%s); reconnecting in %.0fs", type(exc).__name__, delay,
                               exc_info=True)
            if self._closing:
                return
            self._mark_degraded()
            await self._sleep(delay)
            delay = min(delay * 2, _RECONNECT_MAX_SECONDS)

    async def _run_socket(self, ws: Any) -> None:
        self._last_seq = None
        subscribed = False
        async for raw in ws:
            try:
                frame = parse_frame(raw)
            except FrameError:
                logger.debug("[beeper] ignoring malformed frame", exc_info=True)
                continue
            if frame.type == "ready":
                await ws.send(json.dumps({"type": "subscriptions.set", "requestID": "hermes",
                                          "chatIDs": sorted(self._chats)}))
                continue
            if frame.type == "subscriptions.updated" and not subscribed:
                subscribed = True
                self._mark_connected()
                await self.backfill()
                continue
            await self._on_frame(frame)

    async def _on_frame(self, frame: Frame) -> None:
        if frame.type == "error":
            logger.warning("[beeper] WebSocket error frame: %s", frame.error_code)
            return
        gap = frame.seq is not None and self._last_seq is not None and frame.seq != self._last_seq + 1
        if frame.seq is not None:
            self._last_seq = frame.seq
        if gap:
            logger.info("[beeper] event gap detected (seq); backfilling")
            await self.backfill()
        if frame.type == "message.upserted":
            for msg in frame.messages:
                await self._ingest(msg, backfilled=False)
            self._save_cursors()

    async def backfill(self) -> None:
        """Catch up every allowlisted chat from its cursor over REST (no WebSocket replay exists)."""
        assert self._client is not None
        for chat_id in list(self._chats):
            try:
                await self._backfill_chat(chat_id)
            except BeeperApiError as exc:
                logger.warning("[beeper] backfill failed for chat %s: %s", hash_for_log(chat_id), exc)
        self._save_cursors()

    async def _backfill_chat(self, chat_id: str) -> None:
        assert self._client is not None
        cursor = self.gate.cursors.get(chat_id)
        page = await self._client.list_messages(chat_id)
        if cursor is None:  # first sight of this chat: start from now, never answer history
            if newest := max(page.messages, key=lambda m: int(m.sort_key) if m.sort_key.isdigit() else 0, default=None):
                self.gate.advance(newest)
            return
        pending = list(page.messages)
        pages = 1
        while page.has_more and page.oldest_cursor and pages < _BACKFILL_MAX_PAGES and all(
                self._newer_than_cursor(m, cursor) for m in page.messages):
            page = await self._client.list_messages(chat_id, before=page.oldest_cursor)
            pending.extend(page.messages)
            pages += 1
        for msg in sorted({m.id: m for m in pending}.values(), key=lambda m: _sort_value(m.sort_key)):
            await self._ingest(msg, backfilled=True)

    @staticmethod
    def _newer_than_cursor(msg: BeeperMessage, cursor: str) -> bool:
        return sort_key_after(msg.sort_key, cursor)

    def _too_old(self, msg: BeeperMessage) -> bool:
        if msg.timestamp is None:
            return False
        age = (datetime.now(UTC) - msg.timestamp).total_seconds()
        return age > self.settings.max_backfill_age_seconds

    async def _ingest(self, msg: BeeperMessage, *, backfilled: bool) -> None:
        active = self._chats.get(msg.chat_id)
        if active is None:
            return  # not allowlisted (subscriptions are scoped, this is belt and braces)
        if backfilled and self._too_old(msg):
            self.gate.handled_ids.add(msg.id)
            self.gate.advance(msg)
            logger.debug("[beeper] skipped stale backfilled message in chat %s", hash_for_log(msg.chat_id))
            return
        decision = self.gate.evaluate(msg, active.rule)
        if not isinstance(decision, Admit):
            logger.debug("[beeper] dropped message in chat %s: %s", hash_for_log(msg.chat_id), decision.value)
            return
        logger.info("[beeper] dispatching %d chars from chat %s (%s)", len(decision.text),
                    hash_for_log(msg.chat_id), active.rule.label)
        await self._dispatch(msg, decision, active)

    async def _cache_photos(self, msg: BeeperMessage) -> tuple[list[str], list[str], int]:
        """Download the message's photos into Hermes' image cache (owner-only files with random names).
        Returns (paths, MIME types, photos that could not be passed). Stickers, videos and other files
        are left out. Runs before dispatch, so a photo and the text right after it keep their order."""
        if not self.settings.download_images or self._client is None:
            return [], [], 0
        paths: list[str] = []
        types: list[str] = []
        failed = 0
        for attachment in (a for a in msg.attachments if a.is_photo):
            try:
                path = await self._cache_photo(attachment)
            except (BeeperApiError, ValueError, OSError) as exc:
                failed += 1
                logger.warning("[beeper] could not pass a photo from chat %s to the agent; it sees a placeholder: %s",
                               hash_for_log(msg.chat_id), exc)
                continue
            paths.append(path)
            types.append(attachment.mime_type if attachment.mime_type.startswith("image/") else "image/jpeg")
        return paths, types, failed

    async def _cache_photo(self, attachment: Attachment) -> str:
        limit = self.settings.max_image_bytes
        if attachment.file_size is not None and attachment.file_size > limit:
            raise BeeperApiError(f"photo is {attachment.file_size} bytes, over max_image_mb ({limit} bytes)",
                                 code="too_large")
        assert self._client is not None
        data = await self._client.download_asset(attachment.src_url, max_bytes=limit)
        # Raises ValueError for bytes that are not an image (the declared MIME type is not trusted).
        return await cache_image_from_bytes_async(data, ext=_PHOTO_EXT.get(attachment.mime_type, ".jpg"))

    async def _dispatch(self, msg: BeeperMessage, decision: Admit, active: ActiveChat) -> None:
        paths, types, failed = await self._cache_photos(msg) if decision.placeholder else ([], [], 0)
        # Without the file the agent gets the placeholder, as before; with it, the caption alone.
        text = decision.text if paths and not failed else decision.with_placeholder()
        sender = msg.sender_id or (self._own_id if msg.is_sender else "")
        source = self.build_source(
            chat_id=msg.chat_id, chat_name=active.info.title or active.rule.label,
            chat_type="group" if active.info.chat_type == "group" else "dm",
            user_id=sender, user_name=msg.sender_name or None, message_id=msg.id,
            chat_id_alt=active.info.local_id or None)
        event = MessageEvent(text=text, message_type=MessageType.PHOTO if paths else MessageType.TEXT, source=source,
                             message_id=msg.id, media_urls=paths, media_types=types,
                             reply_to_message_id=msg.reply_to_id, timestamp=msg.timestamp or datetime.now(UTC))
        task = asyncio.create_task(self.handle_message(event))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    def _save_cursors(self) -> None:
        try:
            cursor_store.save_cursors(self.settings.base_url,
                                      {k: v for k, v in self.gate.cursors.items() if k in self._chats})
        except OSError:
            logger.warning("[beeper] could not save chat cursors", exc_info=True)

    # --- outbound --------------------------------------------------------------------------

    async def send(self, chat_id: str, content: str, reply_to: str | None = None,
                   metadata: dict[str, Any] | None = None) -> SendResult:
        active = self._chats.get(chat_id) or next(
            (c for c in self._chats.values() if chat_id in (c.info.local_id, c.rule.chat_key)), None)
        if active is None:
            return SendResult(success=False, error="Beeper: chat is not in gateway.platforms.beeper.chats; refusing to send")
        text = (content or "").strip("\r\n").rstrip()  # keep leading spaces: they can be ASCII art
        if not text.strip():
            return SendResult(success=False, error="Beeper send requires text")
        marker = self.settings.reply_marker
        chunks = self.truncate_message(text, self.MAX_MESSAGE_LENGTH - len(marker))
        result = SendResult(success=True)
        for index, chunk in enumerate(chunks):
            # A code fence only opens at the start of a line, so the marker gets its own line before one.
            prefix = f"{marker.rstrip()}\n" if chunk.startswith("```") else marker
            result = await self._send_chunk(active, f"{prefix}{chunk}", reply_to if index == 0 else None)
            if not result.success:
                return result
        return result

    async def _send_chunk(self, active: ActiveChat, text: str, reply_to: str | None) -> SendResult:
        chat_id = active.info.id
        self.gate.breaker.record(chat_id)
        if self.settings.dry_run:
            return self._dry_run_send(active, text, reply_to)
        assert self._client is not None
        await self._pacer.acquire(chat_id)
        try:
            pending_id = await self._client.send_text(chat_id, text, reply_to=reply_to)
        except BeeperApiError as exc:
            return SendResult(success=False, error=str(exc), retryable=exc.retryable)
        self.gate.note_sent(pending_id)
        return SendResult(success=True, message_id=pending_id)

    def _dry_run_send(self, active: ActiveChat, text: str, reply_to: str | None) -> SendResult:
        """Record what would be sent in a private file instead of sending (dry_run: true)."""
        self._dry_run_count += 1
        message_id = f"dry-run-{self._dry_run_count}"
        path = cursor_store.cursor_path().with_name("dry-run.jsonl")
        record = {"at": datetime.now(UTC).isoformat(), "chat": active.rule.label,
                  "chat_id": active.info.id, "reply_to": reply_to, "text": text}
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8", opener=lambda p, f: os.open(p, f, 0o600)) as fh:
                fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        except OSError:
            logger.warning("[beeper] dry-run: could not write %s", path, exc_info=True)
        logger.info("[beeper] dry-run: would send %d chars to %s (recorded in %s)", len(text), active.rule.label, path)
        return SendResult(success=True, message_id=message_id)

    # --- processing reaction (opt-in) ---------------------------------------------------------

    def _processing_reaction(self, chat_id: str) -> str:
        active = self._chats.get(chat_id)
        if active is None:
            return ""
        chat_value = active.rule.processing_reaction
        return chat_value if chat_value is not None else self.settings.processing_reaction

    async def on_processing_start(self, event: MessageEvent) -> None:
        """Show the configured reaction (e.g. 👀) on the triggering message while the agent works."""
        chat_id, message_id = event.source.chat_id or "", event.message_id or ""
        if not (reaction := self._processing_reaction(chat_id)) or not message_id:
            return
        if self.settings.dry_run or self._client is None:
            logger.info("[beeper] dry-run: would react %s in %s", reaction, self._chats[chat_id].rule.label)
            return
        try:
            await self._client.add_reaction(chat_id, message_id, reaction)
        except BeeperApiError as exc:
            logger.warning("[beeper] could not add processing reaction in chat %s: %s", hash_for_log(chat_id), exc)
            return
        self._reacted[(chat_id, message_id)] = reaction

    async def on_processing_complete(self, event: MessageEvent, outcome: ProcessingOutcome) -> None:
        """Remove the processing reaction once the reply is out (or the turn failed or was cancelled)."""
        key = (event.source.chat_id or "", event.message_id or "")
        if (reaction := self._reacted.pop(key, None)) is None or self._client is None:
            return
        try:
            await self._client.remove_reaction(key[0], key[1], reaction)
        except BeeperApiError as exc:
            logger.warning("[beeper] could not remove processing reaction in chat %s: %s", hash_for_log(key[0]), exc)

    async def send_private_notice(self, chat_id: str, user_id: str | None, content: str,
                                  reply_to: str | None = None,
                                  metadata: dict[str, Any] | None = None) -> SendResult:
        """Beeper has no private side channel: gateway notices are logged, never posted into a chat."""
        logger.info("[beeper] gateway notice for chat %s not posted (%d chars): %s", hash_for_log(chat_id),
                    len(content), content.splitlines()[0][:120] if content else "")
        return SendResult(success=True)

    async def send_typing(self, chat_id: str, metadata: dict[str, Any] | None = None) -> None:
        """No typing indicator: it would show friends that the owner is typing."""

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        active = self._chats.get(chat_id)
        if active is None:
            return {"name": chat_id, "type": "dm", "chat_id": chat_id}
        return {"name": active.info.title or active.rule.label,
                "type": "group" if active.info.chat_type == "group" else "dm", "chat_id": chat_id}


def _sort_value(sort_key: str) -> tuple[int, str]:
    return (int(sort_key), "") if sort_key.isdigit() else (0, sort_key)


# --- plugin registration ------------------------------------------------------------------


def check_requirements() -> bool:
    import importlib.util
    if not all(importlib.util.find_spec(mod) for mod in ("httpx", "websockets")):
        return False
    return bool(str(get_scoped_secret(TOKEN_ENV, "") or "").strip())


def validate_config(config: PlatformConfig) -> bool:
    try:
        parse_settings(getattr(config, "extra", None) or {})
    except BeeperConfigError:
        return False
    return bool(str(get_scoped_secret(TOKEN_ENV, "") or "").strip())


def register(ctx) -> None:
    """Plugin entry point, called by the Hermes plugin system."""
    from . import gateway_env
    ctx.register_platform(
        name="beeper", label="Beeper", adapter_factory=lambda cfg: BeeperAdapter(cfg),
        check_fn=check_requirements, validate_config=validate_config, is_connected=validate_config,
        required_env=[TOKEN_ENV],
        install_hint="Requires Beeper Desktop with its API enabled (Settings → Developers) and BEEPER_ACCESS_TOKEN",
        dashboard_description="Use Hermes inside your own chats on any network Beeper Desktop connects.",
        docs_url="https://developers.beeper.com/desktop-api",
        dashboard_env_vars=(TOKEN_ENV,),
        max_message_length=0,  # send() chunks and prefixes every chunk itself
        pii_safe=True, emoji="📟", allow_update_command=False,
        platform_hint=PLATFORM_HINT, display_tier="low",
        **gateway_env.AUTHZ,
    )


__all__ = ["BeeperAdapter", "check_requirements", "register", "validate_config"]
