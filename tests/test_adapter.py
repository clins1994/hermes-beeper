"""Beeper adapter: connect, scoped subscription, intake, outbound safety, dry-run, backfill and cursors.

The Desktop API is replaced by an in-memory client and WebSocket; no network, no sleeps.
"""

import asyncio
import json
import logging
import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from _waiting import async_wait_until
from beeper_platform import cursors as cursor_store
from beeper_platform.adapter import BeeperAdapter, register
from beeper_platform.client import BeeperApiError, ChatInfo, MessagePage
from beeper_platform.events import BeeperMessage
from gateway.config import PlatformConfig
from gateway.platforms.event import MessageType

NOTES = ChatInfo(id="!notes:beeper.local", local_id="101", title="Notes", network="Signal", chat_type="single")
FRIENDS = ChatInfo(id="!friends:beeper.local", local_id="77", title="Friends", network="WhatsApp", chat_type="group")
OTHER = "!other:beeper.local"
OWNER = "@owner:beeper.com"


def _msg(msg_id, chat_id=NOTES.id, *, text="!agent hi", sort_key=None, is_sender=True, sender=OWNER, age_s=0, **extra):
    ts = (datetime.now(UTC) - timedelta(seconds=age_s)).isoformat()
    raw = {"id": str(msg_id), "chatID": chat_id, "senderID": sender, "senderName": "Owner" if is_sender else "Friend",
           "isSender": is_sender, "text": text, "type": "TEXT", "timestamp": ts, "sortKey": str(sort_key or msg_id)}
    raw.update(extra)
    return raw


class FakeClient:
    base_url = "http://127.0.0.1:23373"
    ws_url = "ws://127.0.0.1:23373/v1/ws"
    auth_headers = {"Authorization": "Bearer test-token"}

    def __init__(self, chats=(NOTES, FRIENDS), token_error=None):
        self.chats = {key: c for c in chats for key in (c.id, c.local_id)}
        self.pages: dict[str, list[dict]] = {}
        self.sent: list[tuple[str, str, str | None]] = []
        self.token_error = token_error
        self.reactions: list[tuple[str, str, str, str]] = []
        self.assets: dict[str, bytes | Exception] = {}
        self.downloads: list[tuple[str, int]] = []
        self.uploads: list[tuple[bytes, str, str]] = []
        self.on_send_attachment = None
        self._next_id = 9000

    async def whoami(self):
        if self.token_error:
            raise self.token_error
        return OWNER

    async def get_chat(self, key):
        if key not in self.chats:
            raise BeeperApiError("not found", status=404)
        return self.chats[key]

    async def list_messages(self, chat_id, *, before=None):
        items = sorted(self.pages.get(chat_id, []), key=lambda m: int(m["sortKey"]), reverse=True)
        return MessagePage(messages=tuple(BeeperMessage.from_api(m) for m in items), has_more=False, oldest_cursor=None)

    async def download_asset(self, url, *, max_bytes):
        self.downloads.append((url, max_bytes))
        asset = self.assets.get(url, BeeperApiError("GET /v1/assets/serve -> 500", status=500))
        if isinstance(asset, Exception):
            raise asset
        return asset

    async def send_text(self, chat_id, text, *, reply_to=None):
        self._next_id += 1
        self.sent.append((chat_id, text, reply_to))
        return str(self._next_id)

    async def upload_asset(self, data, *, file_name, mime_type):
        self.uploads.append((data, file_name, mime_type))
        return f"upload-{len(self.uploads)}"

    async def send_attachment(self, chat_id, upload_id, *, text="", reply_to=None):
        if self.on_send_attachment:
            await self.on_send_attachment()
        self._next_id += 1
        self.sent.append((chat_id, f"{text} [{upload_id}]", reply_to))
        return str(self._next_id)

    async def add_reaction(self, chat_id, message_id, key):
        self.reactions.append(("add", chat_id, message_id, key))

    async def remove_reaction(self, chat_id, message_id, key):
        self.reactions.append(("remove", chat_id, message_id, key))

    async def aclose(self):
        pass


class FakeSocket:
    def __init__(self):
        self.frames: asyncio.Queue = asyncio.Queue()
        self.sent: list[dict] = []

    def push(self, frame):
        self.frames.put_nowait(json.dumps(frame))

    async def send(self, data):
        self.sent.append(json.loads(data))

    async def close(self):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.frames.get()
        if item is None:
            raise StopAsyncIteration
        return item


@pytest.fixture(autouse=True)
def _token(monkeypatch):
    for name in ("BEEPER_ACCESS_TOKEN", "BEEPER_ALLOWED_USERS", "BEEPER_ALLOW_ALL_USERS"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("BEEPER_ACCESS_TOKEN", "test-token")


def _adapter(monkeypatch, *, client=None, sockets=None, **extra):
    settings = {"trigger": "!agent", "chats": {"101": {"label": "notes", "mode": "prefix"},
                                              "77": {"label": "friends", "invokers": ["self", "@friend1:beeper.local"],
                                                     "toolsets": ["web"]}}}
    settings.update(extra)
    client = client or FakeClient()
    sockets = sockets if sockets is not None else [FakeSocket()]
    opened = []

    async def ws_connect(url, headers):
        sock = sockets[len(opened)] if len(opened) < len(sockets) else FakeSocket()
        opened.append(sock)
        return sock

    adapter = BeeperAdapter(PlatformConfig(enabled=True, extra=settings), client=client, ws_connect=ws_connect)
    monkeypatch.setattr(adapter, "_acquire_platform_lock", lambda *a, **k: True)
    monkeypatch.setattr(adapter, "_release_platform_lock", lambda: None)
    adapter.handle_message = AsyncMock()
    adapter._sleep = lambda _s: asyncio.Event().wait()  # park the reconnect loop instead of sleeping
    adapter.opened = opened
    return adapter, client, sockets


async def _connected(monkeypatch, **kw):
    adapter, client, sockets = _adapter(monkeypatch, **kw)
    assert await adapter.connect()
    sock = sockets[0]
    sock.push({"type": "ready", "version": 1, "chatIDs": []})
    await async_wait_until(lambda: sock.sent, what="subscriptions.set")
    sock.push({"type": "subscriptions.updated", "chatIDs": sock.sent[0]["chatIDs"]})
    return adapter, client, sock


async def _upsert(adapter, sock, seq, *messages):
    sock.push({"type": "message.upserted", "seq": seq, "ts": "2026-01-01T12:00:00Z", "chatID": messages[0]["chatID"],
               "ids": [m["id"] for m in messages], "entries": list(messages)})
    target = seq
    await async_wait_until(lambda: adapter._last_seq == target, what=f"seq {target}")


def _dispatched(adapter):
    return [call.args[0] for call in adapter.handle_message.await_args_list]


# --- connect ------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connect_without_token_fails_permanently(monkeypatch):
    adapter, _, _ = _adapter(monkeypatch)
    monkeypatch.setattr(adapter, "_token", "")
    assert await adapter.connect() is False
    assert adapter.fatal_error_code == "token_missing"
    assert adapter._fatal_error_retryable is False


@pytest.mark.asyncio
async def test_rejected_token_is_not_retried(monkeypatch):
    adapter, _, _ = _adapter(monkeypatch, client=FakeClient(token_error=BeeperApiError("no", status=401)))
    assert await adapter.connect() is False
    assert (adapter.fatal_error_code, adapter._fatal_error_retryable) == ("token_rejected", False)


@pytest.mark.asyncio
async def test_unreachable_desktop_is_retryable(monkeypatch):
    adapter, _, _ = _adapter(monkeypatch, client=FakeClient(token_error=BeeperApiError("down")))
    assert await adapter.connect() is False
    assert (adapter.fatal_error_code, adapter._fatal_error_retryable) == ("unreachable", True)


@pytest.mark.asyncio
async def test_unknown_chat_in_config_fails_loudly(monkeypatch):
    adapter, _, _ = _adapter(monkeypatch, chats={"404": {"label": "typo"}})
    assert await adapter.connect() is False
    assert adapter.fatal_error_code == "config_invalid"
    assert "'404'" in adapter._fatal_error_message


@pytest.mark.asyncio
async def test_invalid_config_fails_at_connect(monkeypatch):
    adapter, _, _ = _adapter(monkeypatch, reply_marker="")
    assert await adapter.connect() is False
    assert adapter.fatal_error_code == "config_invalid"


@pytest.mark.asyncio
async def test_subscribes_only_to_allowlisted_chats(monkeypatch):
    adapter, _, sock = await _connected(monkeypatch)
    assert sock.sent[0]["type"] == "subscriptions.set"
    assert sorted(sock.sent[0]["chatIDs"]) == sorted([NOTES.id, FRIENDS.id])
    assert "*" not in sock.sent[0]["chatIDs"]
    await adapter.disconnect()


# --- intake -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_owner_trigger_dispatches_stripped_text_as_the_owner(monkeypatch):
    adapter, _, sock = await _connected(monkeypatch)
    await _upsert(adapter, sock, 1, _msg(100, text="!agent what's on today?"))
    await async_wait_until(lambda: adapter.handle_message.await_count == 1)
    (event,) = _dispatched(adapter)
    assert event.text == "what's on today?"
    assert (event.source.chat_id, event.source.chat_type, event.source.user_id) == (NOTES.id, "dm", OWNER)
    assert event.message_id == "100"
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_messages_outside_the_allowlist_are_ignored(monkeypatch):
    adapter, _, sock = await _connected(monkeypatch)
    await _upsert(adapter, sock, 1, _msg(100, chat_id=OTHER))
    assert adapter.handle_message.await_count == 0
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_listed_friend_can_invoke_in_their_group_and_gets_the_chat_toolsets(monkeypatch):
    adapter, _, sock = await _connected(monkeypatch)
    await _upsert(adapter, sock, 1, _msg(100, chat_id=FRIENDS.id, is_sender=False, sender="@friend1:beeper.local"),
                  _msg(101, chat_id=FRIENDS.id, is_sender=False, sender="@friend2:beeper.local"))
    await async_wait_until(lambda: adapter.handle_message.await_count == 1)
    (event,) = _dispatched(adapter)
    assert event.source.user_id == "@friend1:beeper.local"
    assert event.source.chat_type == "group"
    assert adapter.toolsets_for_source(event.source) == ["web"]
    # Invoking in one group grants nothing in DMs.
    assert not adapter._is_dm_allowed("@friend1:beeper.local")
    assert adapter._is_dm_allowed(OWNER)
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_repeated_upserts_dispatch_once(monkeypatch):
    adapter, _, sock = await _connected(monkeypatch)
    await _upsert(adapter, sock, 1, _msg(100, sendStatus={"status": "PENDING"}))
    await _upsert(adapter, sock, 2, _msg(100))
    await _upsert(adapter, sock, 3, _msg(100, seen={OWNER: "2026-01-01T12:00:01Z"}))
    await async_wait_until(lambda: adapter.handle_message.await_count >= 1)
    assert adapter.handle_message.await_count == 1
    await adapter.disconnect()


# --- incoming photos ----------------------------------------------------------------------

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 64
PHOTO_URL = "mxc://local.beeper.com/photo1"
ALWAYS = {"101": {"label": "notes", "mode": "always"}}


def _photo(msg_id, *, caption="", url=PHOTO_URL, **attachment):
    att = {"id": url, "srcURL": url, "type": "img", "mimeType": "image/png", "fileName": "../../../etc/cron.d/x.png",
           "fileSize": len(PNG), "isSticker": False, **attachment}
    return _msg(msg_id, text=caption, type="IMAGE", attachments=[att])


async def _one_event(monkeypatch, message, *, assets=None, **extra):
    client = FakeClient()
    client.assets.update({PHOTO_URL: PNG} if assets is None else assets)
    adapter, _, sock = await _connected(monkeypatch, client=client, **extra)
    await _upsert(adapter, sock, 1, message)
    await async_wait_until(lambda: adapter.handle_message.await_count == 1)
    (event,) = _dispatched(adapter)
    await adapter.disconnect()
    return event, client


@pytest.mark.asyncio
async def test_a_photo_alone_wakes_an_always_chat_and_reaches_the_agent_as_a_private_file(monkeypatch):
    event, client = await _one_event(monkeypatch, _photo(100), chats=ALWAYS)
    assert (event.text, event.message_type, event.media_types) == ("", MessageType.PHOTO, ["image/png"])
    (path,) = event.media_urls
    cache = (Path(os.environ["HERMES_HOME"]) / "cache" / "images").resolve()
    assert Path(path).resolve().parent == cache, "the sender's file name must never shape the path"
    assert Path(path).read_bytes() == PNG
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    assert client.downloads == [(PHOTO_URL, 20 * 1024 * 1024)]


@pytest.mark.asyncio
async def test_a_photo_captioned_with_the_trigger_works_in_prefix_mode(monkeypatch):
    event, _ = await _one_event(monkeypatch, _photo(100, caption="!agent use this for the post"))
    assert (event.text, event.message_type, len(event.media_urls)) == ("use this for the post", MessageType.PHOTO, 1)


@pytest.mark.asyncio
async def test_a_photo_without_the_trigger_is_not_downloaded_in_prefix_mode(monkeypatch):
    client = FakeClient()
    client.assets[PHOTO_URL] = PNG
    adapter, _, sock = await _connected(monkeypatch, client=client)
    await _upsert(adapter, sock, 1, _photo(100, caption="look"))
    assert adapter.handle_message.await_count == 0
    assert client.downloads == []
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_a_failed_download_falls_back_to_the_placeholder_and_warns(monkeypatch, caplog):
    with caplog.at_level(logging.WARNING):
        event, _ = await _one_event(monkeypatch, _photo(100, caption="!agent this one"), assets={})
    assert (event.text, event.message_type, event.media_urls) == ("[attachment: image] this one", MessageType.TEXT, [])
    assert "could not pass a photo" in caplog.text


@pytest.mark.asyncio
async def test_bytes_that_are_not_an_image_are_not_cached(monkeypatch):
    event, _ = await _one_event(monkeypatch, _photo(100), assets={PHOTO_URL: b"<html>error</html>"}, chats=ALWAYS)
    assert (event.text, event.media_urls) == ("[attachment: image]", [])
    assert not list((Path(os.environ["HERMES_HOME"]) / "cache" / "images").glob("*"))


@pytest.mark.asyncio
async def test_a_photo_over_the_size_cap_is_not_downloaded(monkeypatch):
    event, client = await _one_event(monkeypatch, _photo(100, fileSize=3 * 1024 * 1024), chats=ALWAYS, max_image_mb=2)
    assert (event.text, event.media_urls, client.downloads) == ("[attachment: image]", [], [])


@pytest.mark.asyncio
async def test_the_size_cap_is_passed_to_the_download(monkeypatch):
    _, client = await _one_event(monkeypatch, _photo(100, fileSize=None), chats=ALWAYS, max_image_mb=2)
    assert client.downloads == [(PHOTO_URL, 2 * 1024 * 1024)]


@pytest.mark.parametrize("attachment", [{"isSticker": True}, {"type": "video", "mimeType": "video/mp4"},
                                        {"mimeType": "application/pdf"}])
@pytest.mark.asyncio
async def test_stickers_and_other_files_keep_the_placeholder(monkeypatch, attachment):
    event, client = await _one_event(monkeypatch, _photo(100, **attachment), chats=ALWAYS)
    assert (event.media_urls, client.downloads) == ([], [])
    assert event.text.startswith("[attachment: ")


@pytest.mark.asyncio
async def test_photo_downloads_can_be_turned_off(monkeypatch):
    event, client = await _one_event(monkeypatch, _photo(100), chats=ALWAYS, download_images=False)
    assert (event.text, event.media_urls, client.downloads) == ("[attachment: image]", [], [])


# --- outbound -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_prefixes_marker_replies_and_suppresses_its_own_echo(monkeypatch):
    adapter, client, sock = await _connected(monkeypatch)
    result = await adapter.send(NOTES.id, "Done!", reply_to="100")
    assert result.success
    assert client.sent == [(NOTES.id, "🤖 Done!", "100")]
    # The echo of our own reply carries the pendingMessageID; it must never be handled.
    await _upsert(adapter, sock, 1, _msg(result.message_id, text="🤖 Done!", sort_key=99999))
    await _upsert(adapter, sock, 2, _msg(result.message_id, text="!agent sneaky", sort_key=99999))
    assert adapter.handle_message.await_count == 0
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_a_reply_that_opens_with_a_code_block_puts_the_marker_on_its_own_line(monkeypatch):
    adapter, client, sock = await _connected(monkeypatch)
    art = "```\n /\\_/\\\n( o.o )\n```\ncaption"
    result = await adapter.send(NOTES.id, art, reply_to="100")
    assert client.sent == [(NOTES.id, "🤖\n" + art, "100")]
    await _upsert(adapter, sock, 1, _msg("echo-elsewhere", text="🤖\n" + art, sort_key=99999, is_sender=True))
    assert adapter.handle_message.await_count == 0, "the marker on its own line must still mark our echo"
    assert result.success
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_leading_spaces_of_a_reply_are_kept(monkeypatch):
    adapter, client, _ = await _connected(monkeypatch)
    await adapter.send(NOTES.id, "\n   /\\\n  /  \\\n")
    assert client.sent[0][1] == "🤖    /\\\n  /  \\"
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_send_refuses_chats_outside_the_allowlist(monkeypatch):
    adapter, client, _ = await _connected(monkeypatch)
    result = await adapter.send(OTHER, "hello")
    assert not result.success
    assert client.sent == []
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_send_accepts_the_local_chat_id(monkeypatch):
    adapter, client, _ = await _connected(monkeypatch)
    assert (await adapter.send("101", "hi")).success
    assert client.sent[0][0] == NOTES.id
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_long_replies_are_chunked_with_the_marker_on_every_chunk(monkeypatch):
    adapter, client, _ = await _connected(monkeypatch, rate_limit={"per_chat_seconds": 0.001})
    adapter._pacer._sleep = AsyncMock()
    assert (await adapter.send(NOTES.id, "word " * 2000, reply_to="100")).success
    assert len(client.sent) > 1
    assert all(text.startswith("🤖 ") and len(text) <= adapter.MAX_MESSAGE_LENGTH for _, text, _ in client.sent)
    assert [reply for *_, reply in client.sent] == ["100"] + [None] * (len(client.sent) - 1)
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_dry_run_sends_nothing_and_records_the_message(monkeypatch):
    adapter, client, _ = await _connected(monkeypatch, dry_run=True)
    result = await adapter.send(NOTES.id, "would say this", reply_to="100")
    assert result.success and result.message_id.startswith("dry-run-")
    assert client.sent == []
    record = json.loads(cursor_store.cursor_path().with_name("dry-run.jsonl").read_text(encoding="utf-8").splitlines()[-1])
    assert (record["chat"], record["text"], record["reply_to"]) == ("notes", "🤖 would say this", "100")
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_typing_indicator_is_never_sent(monkeypatch):
    adapter, client, _ = await _connected(monkeypatch)
    await adapter.send_typing(NOTES.id)
    assert client.sent == []
    await adapter.disconnect()


# --- backfill, gaps and cursors ------------------------------------------------------------


@pytest.mark.asyncio
async def test_first_connect_starts_from_now_and_never_answers_history(monkeypatch):
    client = FakeClient()
    client.pages[NOTES.id] = [_msg(50, text="!agent old question", age_s=30)]
    adapter, _, _sock = await _connected(monkeypatch, client=client)
    await async_wait_until(lambda: adapter.gate.cursors.get(NOTES.id) == "50")
    assert adapter.handle_message.await_count == 0
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_reconnect_backfills_missed_triggers_but_skips_stale_ones(monkeypatch):
    cursor_store.save_cursors(FakeClient.base_url, {NOTES.id: "50"})
    client = FakeClient()
    client.pages[NOTES.id] = [_msg(40, text="!agent before cursor"), _msg(60, text="!agent missed while offline"),
                              _msg(55, text="!agent hours ago", age_s=7200)]
    adapter, _, _sock = await _connected(monkeypatch, client=client)
    await async_wait_until(lambda: adapter.handle_message.await_count == 1)
    assert [e.text for e in _dispatched(adapter)] == ["missed while offline"]
    assert adapter.gate.cursors[NOTES.id] == "60"
    await adapter.disconnect()
    assert cursor_store.load_cursors(FakeClient.base_url)[NOTES.id] == "60"


@pytest.mark.asyncio
async def test_seq_gap_triggers_a_backfill(monkeypatch):
    adapter, client, sock = await _connected(monkeypatch)
    await _upsert(adapter, sock, 1, _msg(100, text="no trigger"))
    client.pages[NOTES.id] = [_msg(101, text="!agent lost in the gap")]
    await _upsert(adapter, sock, 5, _msg(102, text="also no trigger"))
    await async_wait_until(lambda: adapter.handle_message.await_count == 1)
    assert _dispatched(adapter)[0].text == "lost in the gap"
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_socket_drop_reconnects_and_resubscribes(monkeypatch):
    first, second = FakeSocket(), FakeSocket()
    adapter, _, _ = _adapter(monkeypatch, sockets=[first, second])
    wake = asyncio.Event()

    async def _sleep(_s):
        await wake.wait()
    adapter._sleep = _sleep
    assert await adapter.connect()
    first.push({"type": "ready", "version": 1, "chatIDs": []})
    await async_wait_until(lambda: first.sent)
    first.frames.put_nowait(None)  # server closes the socket
    await async_wait_until(lambda: adapter._ws_task is not None and len(adapter.opened) == 1)
    wake.set()
    await async_wait_until(lambda: len(adapter.opened) == 2)
    second.push({"type": "ready", "version": 1, "chatIDs": []})
    await async_wait_until(lambda: second.sent)
    assert second.sent[0]["type"] == "subscriptions.set"
    await adapter.disconnect()


# --- registration and pairing safety ------------------------------------------------------


def test_registration_suppresses_pairing_codes():
    """An unknown DM must never get a pairing code: it would be sent from the owner's account."""
    captured = {}

    class _Ctx:
        def register_platform(self, **kwargs):
            captured.update(kwargs)

    register(_Ctx())
    assert captured["name"] == "beeper"
    assert captured["allowlist_suppresses_pairing"] is True
    assert captured.get("trusted_inbound") is not True
    adapter = captured["adapter_factory"](PlatformConfig(enabled=True, extra={}))
    assert adapter._dm_policy == "allowlist"
    assert not adapter._is_dm_allowed("@stranger:beeper.local")


# --- the runner's own re-check (native admission runs it without adapter delegation) -------


def _runner_for(adapter):
    from gateway.config import GatewayConfig, Platform
    from gateway.pairing import PairingStore
    from gateway.run import GatewayRunner

    runner = object.__new__(GatewayRunner)
    runner.config = GatewayConfig(platforms={Platform("beeper"): adapter.config})
    runner.pairing_store = PairingStore(profile="default")
    runner.pairing_stores = {"default": runner.pairing_store}
    runner._primary_profile_name = "default"
    runner._profile_adapters = {}
    runner.adapters = {Platform("beeper"): adapter}
    return runner


@pytest.mark.asyncio
async def test_runner_admits_exactly_what_the_gate_admits(monkeypatch):
    """Regression: the gate admitted the owner, then native admission
    refused the turn as user_not_authorized and the gateway answered with an error message."""
    adapter, _, sock = await _connected(monkeypatch)
    await _upsert(adapter, sock, 1, _msg(100, text="!agent hi"))
    await _upsert(adapter, sock, 2, _msg(200, chat_id=FRIENDS.id, is_sender=False, sender="@friend1:beeper.local"))
    await async_wait_until(lambda: adapter.handle_message.await_count == 2)
    runner = _runner_for(adapter)
    for event in _dispatched(adapter):
        assert runner._is_user_authorized_for_source(event.source, allow_adapter_delegation=False), event.source.user_id
    stranger = adapter.build_source(chat_id=FRIENDS.id, chat_type="group", user_id="@stranger:beeper.local")
    assert not runner._is_user_authorized_for_source(stranger, allow_adapter_delegation=False)
    dm_stranger = adapter.build_source(chat_id=NOTES.id, chat_type="dm", user_id="@friend1:beeper.local")
    assert not runner._is_user_authorized_for_source(dm_stranger, allow_adapter_delegation=False)
    assert runner._get_unauthorized_dm_behavior(adapter.platform) == "ignore"
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_gateway_notices_are_never_posted_into_chats(monkeypatch):
    """Regression: the "no home channel set" notice was about to be posted
    into the chat from the owner's account."""
    adapter, client, _ = await _connected(monkeypatch)
    runner = _runner_for(adapter)
    runner._delivery_adapter_for = lambda _source: adapter
    runner._thread_metadata_for_source = lambda _source: None
    source = adapter.build_source(chat_id=NOTES.id, chat_type="dm", user_id=OWNER)
    await runner._deliver_platform_notice(source, "📬 No home channel is set for Beeper.")
    assert client.sent == []
    await adapter.disconnect()


# --- processing reaction ------------------------------------------------------------------


async def _processing_round_trip(monkeypatch, **extra):
    from gateway.platforms.event import ProcessingOutcome
    adapter, client, sock = await _connected(monkeypatch, **extra)
    await _upsert(adapter, sock, 1, _msg(100))
    await async_wait_until(lambda: adapter.handle_message.await_count == 1)
    (event,) = _dispatched(adapter)
    await adapter.on_processing_start(event)
    shown = list(client.reactions)
    await adapter.on_processing_complete(event, ProcessingOutcome.SUCCESS)
    await adapter.disconnect()
    return shown, client.reactions


@pytest.mark.asyncio
async def test_processing_reaction_is_off_by_default(monkeypatch):
    shown, final = await _processing_round_trip(monkeypatch)
    assert shown == final == []


@pytest.mark.asyncio
async def test_processing_reaction_shows_while_working_and_is_removed_after(monkeypatch):
    shown, final = await _processing_round_trip(monkeypatch, processing_reaction="👀")
    assert shown == [("add", NOTES.id, "100", "👀")]
    assert final == [("add", NOTES.id, "100", "👀"), ("remove", NOTES.id, "100", "👀")]


@pytest.mark.asyncio
async def test_chat_can_turn_the_processing_reaction_off(monkeypatch):
    chats = {"101": {"label": "notes", "processing_reaction": ""}}
    shown, final = await _processing_round_trip(monkeypatch, processing_reaction="👀", chats=chats)
    assert shown == final == []


@pytest.mark.asyncio
async def test_dry_run_never_reacts(monkeypatch):
    shown, final = await _processing_round_trip(monkeypatch, processing_reaction="👀", dry_run=True)
    assert shown == final == []


@pytest.mark.asyncio
async def test_only_the_owner_can_run_slash_commands(monkeypatch):
    """Friends invoking the agent in a group must not run /model, /new, /sethome and the like."""
    from gateway.slash_access import policy_for_runner_source
    adapter, _, _ = await _connected(monkeypatch)
    runner = _runner_for(adapter)
    runner._delivery_adapter_for = lambda _source: adapter
    friend = adapter.build_source(chat_id=FRIENDS.id, chat_type="group", user_id="@friend1:beeper.local")
    owner = adapter.build_source(chat_id=FRIENDS.id, chat_type="group", user_id=OWNER)
    assert not policy_for_runner_source(runner, friend).can_run(friend.user_id, "model")
    assert policy_for_runner_source(runner, friend).can_run(friend.user_id, "help")
    assert policy_for_runner_source(runner, owner).can_run(OWNER, "model")
    await adapter.disconnect()


# --- outbound photos ----------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_image_file_uploads_the_photo_with_the_marker_caption_and_ignores_its_echo(monkeypatch, tmp_path):
    adapter, client, sock = await _connected(monkeypatch)
    photo = tmp_path / "cover.jpg"
    photo.write_bytes(b"\xff\xd8jpeg")
    result = await adapter.send_image_file(NOTES.id, str(photo), caption="Is this crop OK?", reply_to="100")
    assert result.success
    assert client.uploads == [(b"\xff\xd8jpeg", "cover.jpg", "image/jpeg")]
    assert client.sent == [(NOTES.id, "🤖 Is this crop OK? [upload-1]", "100")]
    await _upsert(adapter, sock, 1, _msg(result.message_id, text="🤖 Is this crop OK?", sort_key=99999))
    assert adapter.handle_message.await_count == 0
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_send_image_file_without_a_caption_sends_the_photo_alone(monkeypatch, tmp_path):
    adapter, client, _ = await _connected(monkeypatch)
    photo = tmp_path / "a.png"
    photo.write_bytes(b"png")
    assert (await adapter.send_image_file(NOTES.id, str(photo))).success
    assert (await adapter.send_image_file(NOTES.id, str(photo), caption="🤖")).success
    assert client.sent == [(NOTES.id, " [upload-1]", None), (NOTES.id, " [upload-2]", None)]
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_send_image_file_refuses_other_chats_and_files_that_are_not_photos(monkeypatch, tmp_path):
    adapter, client, _ = await _connected(monkeypatch)
    photo, notes = tmp_path / "a.jpg", tmp_path / "notes.txt"
    photo.write_bytes(b"jpg")
    notes.write_text("secret")
    assert not (await adapter.send_image_file(OTHER, str(photo))).success
    assert not (await adapter.send_image_file(NOTES.id, str(notes))).success
    assert not (await adapter.send_image_file(NOTES.id, str(tmp_path / "missing.jpg"))).success
    assert (client.uploads, client.sent) == ([], [])
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_the_echo_of_a_sent_photo_never_wakes_the_agent_even_as_a_new_message_without_caption(monkeypatch, tmp_path):
    # LINE splits a photo and its caption: the photo comes back with a new ID and no text, from the owner's account.
    adapter, _, sock = await _connected(monkeypatch, chats=ALWAYS)
    photo = tmp_path / "cover.jpg"
    photo.write_bytes(PNG)
    assert (await adapter.send_image_file(NOTES.id, str(photo), caption="Is this crop OK?")).success
    await _upsert(adapter, sock, 1, _photo("echo-photo", sort_key=99998, fileName="cover.jpg", fileSize=len(PNG)))
    await _upsert(adapter, sock, 2, _msg("echo-caption", text="🤖", sort_key=99999))
    assert adapter.handle_message.await_count == 0
    # The owner's own, different photo still wakes the agent.
    await _upsert(adapter, sock, 3, _photo("owner-photo", sort_key=100000, fileName="IMG_1.jpg", fileSize=len(PNG)))
    await async_wait_until(lambda: adapter.handle_message.await_count == 1)
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_the_echo_of_a_photo_is_ignored_even_when_it_arrives_before_the_send_returns(monkeypatch, tmp_path):
    adapter, client, sock = await _connected(monkeypatch, chats=ALWAYS)
    photo = tmp_path / "cover.jpg"
    photo.write_bytes(PNG)

    async def _echo_first():  # LINE echoes the photo while the send request still waits
        await _upsert(adapter, sock, 1, _photo("echo-photo", sort_key=99998, fileName="cover.jpg", fileSize=len(PNG)))
    client.on_send_attachment = _echo_first
    assert (await adapter.send_image_file(NOTES.id, str(photo))).success
    assert adapter.handle_message.await_count == 0
    await adapter.disconnect()


@pytest.mark.asyncio
async def test_a_reply_that_already_starts_with_the_marker_gets_it_once(monkeypatch, tmp_path):
    adapter, client, _ = await _connected(monkeypatch)
    await adapter.send(NOTES.id, "🤖 🤖 Done!")
    photo = tmp_path / "a.jpg"
    photo.write_bytes(b"jpg")
    await adapter.send_image_file(NOTES.id, str(photo), caption="🤖 Here it is")
    assert [text for _, text, _ in client.sent] == ["🤖 Done!", "🤖 Here it is [upload-1]"]
    assert not (await adapter.send(NOTES.id, "🤖")).success, "a reply that is only the marker sends nothing"
    await adapter.disconnect()
