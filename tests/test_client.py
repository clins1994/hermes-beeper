"""Outbound text: the send endpoint parses Markdown, so replies are escaped to arrive as written.
Media downloads: only Beeper media URLs, streamed under a size cap."""

import re

import httpx
import pytest
from beeper_platform.client import BeeperApiError, BeeperClient, literal_text

# What a CommonMark parser does with backslash escapes: drop the backslash before ASCII punctuation.
_UNESCAPE = re.compile(r"\\([!-/:-@\[-`{-~])")

ASCII_WOLF = "/\\     /\\\n /  \\___/  \\\n |  ●   ●  |\n  \\   V   /\n   \\ \\_/ /\n    `---´\n   awoooo"


def test_ascii_art_survives_markdown_parsing() -> None:
    sent = literal_text(ASCII_WOLF)
    assert _UNESCAPE.sub(r"\1", sent).replace("\u2800", " ") == ASCII_WOLF
    assert "\\\n" not in _UNESCAPE.sub(r"\1", sent.replace("\\\\", "")), "a line-final backslash must stay escaped"


def test_formatting_characters_are_escaped() -> None:
    assert literal_text("*bold* _it_ ~st~ [x](y) <b>") == "\\*bold\\* \\_it\\_ \\~st\\~ \\[x\\]\\(y\\) \\<b\\>"


def test_space_runs_and_leading_spaces_become_blanks() -> None:
    assert literal_text("  a  b c") == "\u2800\u2800a\u2800\u2800b c"


def test_fenced_code_blocks_pass_through_for_monospace() -> None:
    text = "olha:\n```\n" + ASCII_WOLF + "\n```\n*fim*"
    assert literal_text(text) == "olha\\:\n```\n" + ASCII_WOLF + "\n```\n\\*fim\\*"


def test_plain_text_and_non_ascii_pass_through() -> None:
    assert literal_text("oi mano, tudo bem? 🐺 ção") == "oi mano\\, tudo bem\\? 🐺 ção"


def test_a_fence_in_the_middle_of_a_line_is_not_a_code_block() -> None:
    assert literal_text("x ```\na\\b\n```") == "x \\`\\`\\`\na\\\\b\n\\`\\`\\`"


# --- media downloads ------------------------------------------------------------------------

PNG = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100


def _client(handler) -> tuple[BeeperClient, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def _record(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return handler(request)
    http = httpx.AsyncClient(transport=httpx.MockTransport(_record))
    return BeeperClient("http://127.0.0.1:23373", "test-token", http=http), seen


@pytest.mark.asyncio
async def test_download_asset_streams_from_the_serve_endpoint() -> None:
    client, seen = _client(lambda _r: httpx.Response(200, content=PNG))
    assert await client.download_asset("mxc://local.beeper.com/abc", max_bytes=1024) == PNG
    (request,) = seen
    assert (request.method, request.url.path) == ("GET", "/v1/assets/serve")
    assert request.url.params["url"] == "mxc://local.beeper.com/abc"
    assert request.headers["authorization"] == "Bearer test-token"


@pytest.mark.parametrize("url", ["file:///etc/passwd", "https://example.com/a.png", "/home/user/.ssh/id_rsa", ""])
@pytest.mark.asyncio
async def test_download_asset_refuses_anything_but_beeper_media_urls(url) -> None:
    client, seen = _client(lambda _r: httpx.Response(200, content=PNG))
    with pytest.raises(BeeperApiError) as exc:
        await client.download_asset(url, max_bytes=1024)
    assert exc.value.code == "bad_url"
    assert seen == []


@pytest.mark.asyncio
async def test_download_asset_rejects_a_declared_size_over_the_cap() -> None:
    client, _ = _client(lambda _r: httpx.Response(200, content=PNG))  # Content-Length: 108
    with pytest.raises(BeeperApiError) as exc:
        await client.download_asset("localmxc://x/y", max_bytes=50)
    assert exc.value.code == "too_large"


@pytest.mark.asyncio
async def test_download_asset_stops_reading_an_undeclared_stream_past_the_cap() -> None:
    def _endless(_r):
        async def body():
            for _ in range(1000):
                yield b"x" * 1024
        return httpx.Response(200, content=body())  # streamed: no Content-Length
    client, _ = _client(_endless)
    with pytest.raises(BeeperApiError) as exc:
        await client.download_asset("mxc://x/y", max_bytes=10 * 1024)
    assert exc.value.code == "too_large"


@pytest.mark.asyncio
async def test_download_asset_reports_api_errors() -> None:
    client, _ = _client(lambda _r: httpx.Response(500, json={"message": "ENOENT", "code": "UNKNOWN_ERROR"}))
    with pytest.raises(BeeperApiError) as exc:
        await client.download_asset("mxc://x/missing", max_bytes=1024)
    assert exc.value.status == 500
