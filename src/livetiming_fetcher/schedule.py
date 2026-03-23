import sys
import os
from dataclasses import dataclass
from typing import List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from shared import SESSION_TYPE_ORDINALS, make_event_id, make_session_id  # noqa: E402,F401

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import client  # noqa: E402


@dataclass
class SessionInfo:
    year: int
    round_number: int
    country: str
    location: str
    event_name: str
    event_date: str
    event_format: str
    session_type: str
    session_name: str
    session_path: str
    event_id: int
    session_id: int
    gmt_offset: str
    start_date: str
    end_date: str


# Map livetiming session Name to the session_type string used by our schema
_NAME_TO_SESSION_TYPE = {
    "Practice 1": "Practice 1",
    "Practice 2": "Practice 2",
    "Practice 3": "Practice 3",
    "Qualifying": "Qualifying",
    "Sprint Qualifying": "Sprint Qualifying",
    "Sprint Shootout": "Sprint Shootout",
    "Sprint": "Sprint",
    "Race": "Race",
}


def _is_testing(meeting):
    # type: (dict) -> bool
    code = meeting.get("Code", "")
    name = meeting.get("Name", "")
    return "T" in code.replace("F1", "").replace(str(meeting.get("Number", "")), "") or "Testing" in name


def _detect_event_format(sessions):
    # type: (list) -> str
    names = {s.get("Name", "") for s in sessions}
    if "Sprint" in names or "Sprint Qualifying" in names or "Sprint Shootout" in names:
        return "sprint"
    return "conventional"


def get_season_sessions(year):
    # type: (int) -> List[SessionInfo]
    """Fetch the season calendar and return SessionInfo for each race session."""
    cal = client.fetch_schedule(year)
    results = []
    round_counter = 0

    for meeting in cal.get("Meetings", []):
        if _is_testing(meeting):
            continue

        round_counter += 1
        round_number = meeting.get("Number", round_counter)
        country = meeting.get("Country", {}).get("Name", "")
        location = meeting.get("Location", "")
        event_name = meeting.get("Name", "")
        event_format = _detect_event_format(meeting.get("Sessions", []))

        # Use the last session date as event_date
        sessions = meeting.get("Sessions", [])
        event_date = sessions[-1].get("StartDate", "") if sessions else ""

        event_id = make_event_id(year, round_number)

        for sess in sessions:
            session_name = sess.get("Name", "")
            session_type = _NAME_TO_SESSION_TYPE.get(session_name)
            if session_type is None:
                continue
            session_id = make_session_id(event_id, session_type)
            results.append(SessionInfo(
                year=year,
                round_number=round_number,
                country=country,
                location=location,
                event_name=event_name,
                event_date=event_date,
                event_format=event_format,
                session_type=session_type,
                session_name=session_name,
                session_path=sess.get("Path", ""),
                event_id=event_id,
                session_id=session_id,
                gmt_offset=sess.get("GmtOffset", ""),
                start_date=sess.get("StartDate", ""),
                end_date=sess.get("EndDate", ""),
            ))

    return results
