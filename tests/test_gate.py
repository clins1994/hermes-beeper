"""Intake gate: trigger, invokers and every loop-prevention layer, on synthetic and recorded traffic."""

import pytest
from beeper_platform.config import RateLimits, parse_settings
from beeper_platform.gate import Admit, Drop, IntakeGate, ReplyBreaker, strip_trigger


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _gate(**limits) -> IntakeGate:
    return IntakeGate(reply_marker="🤖 ", breaker=ReplyBreaker(RateLimits(**limits), clock=_Clock()))


def _rule(**chat):
    (rule,) = parse_settings({"trigger": "!agent", "chats": {"!chat1:beeper.local": chat}}).chats
    return rule


@pytest.mark.parametrize("text, expected", [
    ("!agent what's up", "what's up"),
    ("  !AGENT, summarise this", "summarise this"),
    ("!agent", ""),
    ("!agentx is a band", None),
    ("hey !agent", None),
])
def test_strip_trigger(text, expected):
    assert strip_trigger(text, "!agent") == expected


def test_owner_trigger_is_admitted_with_trigger_stripped(make_message):
    assert _gate().evaluate(make_message(text="!agent book a table"), _rule()) == Admit(text="book a table")


def test_untriggered_message_is_ignored_in_prefix_mode(make_message):
    assert _gate().evaluate(make_message(text="see you soon"), _rule()) is Drop.NO_TRIGGER


def test_always_mode_handles_untriggered_owner_messages(make_message):
    assert _gate().evaluate(make_message(text="remind me at 5"), _rule(mode="always")) == Admit(text="remind me at 5")


def test_friend_cannot_invoke_unless_listed(make_message):
    friend = make_message(senderID="@friend1:beeper.local", isSender=False)
    assert _gate().evaluate(friend, _rule()) is Drop.INVOKER
    assert isinstance(_gate().evaluate(friend, _rule(invokers=["self", "@friend1:beeper.local"])), Admit)


def test_agent_reply_echo_is_dropped_by_sent_id(make_message):
    gate = _gate()
    gate.note_sent("2000")
    # Even if the echo somehow carried the trigger, the remembered pendingMessageID wins.
    assert gate.evaluate(make_message(id="2000", text="!agent loop?", sortKey="6000"), _rule()) is Drop.ECHO


def test_own_message_with_reply_marker_is_dropped(make_message):
    assert _gate().evaluate(make_message(text="🤖 !agent I am the agent"), _rule(mode="always")) is Drop.MARKER


def test_own_reply_echoed_back_as_html_is_dropped(make_message):
    # LINE echoed formatted replies as HTML, so the marker check missed them and the agent answered itself.
    echoed = make_message(text="<p>🤖 えっと、わたしはスロスだよ〜</p><p>次の段落</p>")
    assert _gate().evaluate(echoed, _rule(mode="always")) is Drop.MARKER


def test_html_message_text_reaches_the_agent_as_plain_text(make_message):
    msg = make_message(text="<p>!agent first line<br>second &amp; third</p><p>next</p>")
    assert msg.text == "!agent first line\nsecond & third\nnext"
    assert make_message(text="a < b and <3").text == "a < b and <3"


def test_friend_message_starting_with_marker_is_not_mistaken_for_the_agent(make_message):
    friend = make_message(senderID="@friend1:beeper.local", isSender=False, text="🤖 !agent hi")
    assert _gate().evaluate(friend, _rule(invokers="*")) is Drop.NO_TRIGGER


def test_repeated_upserts_of_one_message_are_handled_once(make_message):
    gate = _gate()
    first = make_message(sendStatus={"status": "PENDING"})
    assert isinstance(gate.evaluate(first, _rule()), Admit)
    assert gate.evaluate(make_message(), _rule()) is Drop.DUPLICATE


def test_upsert_of_an_old_message_does_not_retrigger(make_message):
    gate = _gate()
    gate.cursors["!chat1:beeper.local"] = "5000"
    assert gate.evaluate(make_message(id="999", sortKey="4999"), _rule()) is Drop.OLD


@pytest.mark.parametrize("msg_type", ["REACTION", "NOTICE", "SOMETHING_NEW"])
def test_non_content_types_never_wake_the_agent(msg_type, make_message):
    assert _gate().evaluate(make_message(type=msg_type), _rule(mode="always")) is Drop.TYPE


def test_media_gets_a_placeholder(make_message):
    decision = _gate().evaluate(make_message(type="IMAGE", text="", attachments=[{}]), _rule(mode="always"))
    assert decision == Admit(text="[attachment: image]")


def test_breaker_pauses_a_chat_after_too_many_replies(make_message):
    gate = _gate(breaker_replies=2)
    for _ in range(2):
        gate.breaker.record("!chat1:beeper.local")
    assert gate.evaluate(make_message(), _rule()) is Drop.BREAKER


def test_breaker_window_slides():
    clock = _Clock()
    breaker = ReplyBreaker(RateLimits(breaker_replies=1, breaker_window_seconds=60), clock=clock)
    breaker.record("c")
    assert breaker.tripped("c")
    clock.now += 61
    assert not breaker.tripped("c")


def _replay(rule_by_chat, ws_frames) -> list[tuple[str, Admit]]:
    gate = _gate()
    admitted = []
    for frame in ws_frames:
        for msg in frame.messages:
            if (rule := rule_by_chat(msg.chat_id)) is not None:
                decision = gate.evaluate(msg, rule)
                if isinstance(decision, Admit):
                    admitted.append((msg.id, decision))
    return admitted


def test_recorded_session_never_wakes_a_prefix_chat(ws_frames):
    """270 real frames (DMs, groups, replies, reactions, media, own sends): nothing carries the trigger."""
    assert _replay(lambda _chat: _rule(invokers="*"), ws_frames) == []


def test_recorded_session_in_always_mode_admits_each_owner_message_once_and_no_reactions(ws_frames, owner_id):
    admitted = _replay(lambda _chat: _rule(mode="always"), ws_frames)
    ids = [msg_id for msg_id, _ in admitted]
    assert ids, "the capture contains owner messages"
    assert len(ids) == len(set(ids))
    own_reactions = {m.id for f in ws_frames for m in f.messages if m.is_sender and m.type == "REACTION"}
    assert own_reactions and not own_reactions & set(ids)
    assert all(owner_id not in d.text for _, d in admitted)
