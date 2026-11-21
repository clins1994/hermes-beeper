"""Beeper Desktop API REST client and outbound pacing.

Only the endpoints the platform needs: token check, chat lookup, message pages (backfill) and
text sends. Every request carries the Bearer token; the token never appears in errors or logs.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import httpx

from .config import RateLimits
from .events import BeeperMessage

_RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class BeeperApiError(Exception):
    """A Desktop API request failed. ``status`` is None for transport errors (API unreachable)."""

    def __init__(self, message: str, *, status: int | None = None, code: str | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.code = code

    @property
    def retryable(self) -> bool:
        return self.status is None or self.status in _RETRYABLE_STATUS

    @property
    def unauthorized(self) -> bool:
        return self.status in (401, 403)


@dataclass(frozen=True)
class ChatInfo:
    id: str
    local_id: str
    title: str
    network: str
    chat_type: str  # "single" or "group"


@dataclass(frozen=True)
class MessagePage:
    messages: tuple[BeeperMessage, ...]
    has_more: bool
    oldest_cursor: str | None


class BeeperClient:
    def __init__(self, base_url: str, token: str, *, http: httpx.AsyncClient | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self._token = token
        self._http = http or httpx.AsyncClient(timeout=httpx.Timeout(30.0, connect=5.0))

    @property
    def ws_url(self) -> str:
        scheme, rest = self.base_url.split("://", 1)
        return f"{'wss' if scheme == 'https' else 'ws'}://{rest}/v1/ws"

    @property
    def auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    async def aclose(self) -> None:
        await self._http.aclose()

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        try:
            resp = await self._http.request(method, f"{self.base_url}{path}", headers=self.auth_headers, **kwargs)
        except httpx.HTTPError as exc:
            raise BeeperApiError(f"Beeper Desktop API unreachable at {self.base_url}: {type(exc).__name__}") from None
        if resp.status_code >= 400:
            code, message = None, resp.reason_phrase
            try:
                body = resp.json()
                if isinstance(body, dict):
                    code, message = body.get("code"), body.get("message") or message
            except ValueError:
                pass
            raise BeeperApiError(f"{method} {path.split('?')[0]} -> {resp.status_code} {message}",
                                 status=resp.status_code, code=code)
        return resp.json() if resp.content else None

    async def whoami(self) -> str:
        """The account's own user ID (``sub``); every own message carries it as ``senderID``.
        Raises BeeperApiError (``unauthorized``) when the token is rejected."""
        data = await self._request("GET", "/oauth/userinfo")
        sub = data.get("sub") if isinstance(data, dict) else None
        if not sub:
            raise BeeperApiError("token check returned no user ID", status=200)
        return str(sub)

    async def get_chat(self, chat_key: str) -> ChatInfo:
        data = await self._request("GET", f"/v1/chats/{quote(chat_key, safe='')}")
        if not isinstance(data, dict) or not data.get("id"):
            raise BeeperApiError(f"chat {chat_key!r} returned no ID")
        return ChatInfo(
            id=str(data["id"]), local_id=str(data.get("localChatID") or ""), title=str(data.get("title") or ""),
            network=str(data.get("network") or ""), chat_type=str(data.get("type") or "single"))

    async def list_messages(self, chat_id: str, *, before: str | None = None) -> MessagePage:
        """Newest-first page of a chat's messages; ``before`` pages further back."""
        params = {"cursor": before, "direction": "before"} if before else None
        data = await self._request("GET", f"/v1/chats/{quote(chat_id, safe='')}/messages", params=params)
        items = data.get("items") if isinstance(data, dict) else None
        messages = tuple(m for raw in (items or []) if isinstance(raw, dict) and (m := BeeperMessage.from_api(raw)))
        return MessagePage(messages=messages, has_more=bool(data.get("hasMore")) if isinstance(data, dict) else False,
                           oldest_cursor=str(data["oldestCursor"]) if isinstance(data, dict) and data.get("oldestCursor") else None)

    async def send_text(self, chat_id: str, text: str, *, reply_to: str | None = None) -> str:
        """Send text; returns the ``pendingMessageID`` (the local ID the echo will carry)."""
        body: dict[str, Any] = {"text": text}
        if reply_to:
            body["replyToMessageID"] = reply_to
        data = await self._request("POST", f"/v1/chats/{quote(chat_id, safe='')}/messages", json=body)
        pending = data.get("pendingMessageID") if isinstance(data, dict) else None
        if not pending:
            raise BeeperApiError("send returned no pendingMessageID", status=200)
        return str(pending)


    async def add_reaction(self, chat_id: str, message_id: str, reaction_key: str) -> None:
        await self._request("POST", f"/v1/chats/{quote(chat_id, safe='')}/messages/{quote(message_id, safe='')}/reactions",
                            json={"reactionKey": reaction_key})

    async def remove_reaction(self, chat_id: str, message_id: str, reaction_key: str) -> None:
        await self._request("DELETE", f"/v1/chats/{quote(chat_id, safe='')}/messages/{quote(message_id, safe='')}"
                                      f"/reactions/{quote(reaction_key, safe='')}")


class SendPacer:
    """Spaces sends: one per chat every ``per_chat_seconds`` and ``global_per_minute`` overall."""

    def __init__(self, limits: RateLimits, *, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], Awaitable[None]] = asyncio.sleep) -> None:
        self._limits = limits
        self._clock = clock
        self._sleep = sleep
        self._last_by_chat: dict[str, float] = {}
        self._recent: deque[float] = deque()
        self._lock = asyncio.Lock()

    def _wait_for(self, chat_id: str, now: float) -> float:
        while self._recent and self._recent[0] <= now - 60.0:
            self._recent.popleft()
        wait = 0.0
        if (last := self._last_by_chat.get(chat_id)) is not None:
            wait = max(wait, last + self._limits.per_chat_seconds - now)
        if len(self._recent) >= self._limits.global_per_minute:
            wait = max(wait, self._recent[0] + 60.0 - now)
        return wait

    async def acquire(self, chat_id: str) -> None:
        async with self._lock:
            while (wait := self._wait_for(chat_id, self._clock())) > 0:
                await self._sleep(wait)
            now = self._clock()
            self._last_by_chat[chat_id] = now
            self._recent.append(now)
