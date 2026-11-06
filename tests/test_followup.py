from comment_to_inbox.followup import silence_reminder_allowed


def test_silence_after_the_private_reply_does_not_allow_a_reminder():
    assert silence_reminder_allowed(
        waiting_for_email=True,
        opted_out=False,
        messaging_window_open=True,
        opened_only_by_private_reply=True,
    ) is False


def test_reminder_requires_an_open_window_and_no_opt_out():
    assert silence_reminder_allowed(
        waiting_for_email=True,
        opted_out=False,
        messaging_window_open=True,
        opened_only_by_private_reply=False,
    ) is True
    assert silence_reminder_allowed(
        waiting_for_email=True,
        opted_out=True,
        messaging_window_open=True,
        opened_only_by_private_reply=False,
    ) is False
    assert silence_reminder_allowed(
        waiting_for_email=True,
        opted_out=False,
        messaging_window_open=False,
        opened_only_by_private_reply=False,
    ) is False
    assert silence_reminder_allowed(
        waiting_for_email=False,
        opted_out=False,
        messaging_window_open=True,
        opened_only_by_private_reply=False,
    ) is False
