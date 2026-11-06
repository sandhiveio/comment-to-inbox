def silence_reminder_allowed(
    *,
    waiting_for_email: bool,
    opted_out: bool,
    messaging_window_open: bool,
    opened_only_by_private_reply: bool,
) -> bool:
    """Gate for a future silence reminder. The worker never calls this.

    ``opened_only_by_private_reply`` is true when the person has not sent a
    message after the initial private reply. That silence does not open
    Instagram's messaging window, so the reminder is refused even if a caller
    incorrectly says the window is open.
    """
    if opened_only_by_private_reply:
        return False
    if opted_out or not waiting_for_email:
        return False
    return messaging_window_open
