"""Outbound text: the send endpoint parses Markdown, so replies are escaped to arrive as written."""

import re

from beeper_platform.client import literal_text

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
