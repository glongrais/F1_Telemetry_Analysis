# Shared constants and ID helpers used by both fastf1_fetcher and livetiming_fetcher.

SESSION_TYPE_ORDINALS = {
    "Practice 1": 1,
    "Practice 2": 2,
    "Practice 3": 3,
    "Qualifying": 4,
    "Sprint Qualifying": 5,
    "Sprint Shootout": 5,
    "Sprint": 6,
    "Race": 7,
}


def make_event_id(year, round_number):
    # type: (int, int) -> int
    """Deterministic event ID: year * 100 + round_number."""
    return year * 100 + round_number


def make_session_id(event_id, session_type):
    # type: (int, str) -> int
    """Deterministic session ID: event_id * 10 + ordinal."""
    ordinal = SESSION_TYPE_ORDINALS.get(session_type, 0)
    return event_id * 10 + ordinal
