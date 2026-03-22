import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional

BASE_URL = "https://livetiming.formula1.com/static"


def _session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s


_http = _session()


def fetch_schedule(year):
    # type: (int) -> dict
    """Fetch the season calendar from the livetiming API."""
    r = _http.get(f"{BASE_URL}/{year}/Index.json", timeout=30)
    r.raise_for_status()
    return json.loads(r.content.decode("utf-8-sig"))


def fetch_session_index(session_path):
    # type: (str) -> dict
    """Fetch the topic index for a session. session_path has trailing slash."""
    url = f"{BASE_URL}/{session_path}Index.json"
    r = _http.get(url, timeout=30)
    r.raise_for_status()
    return json.loads(r.content.decode("utf-8-sig"))


def fetch_topic(session_path, stream_path):
    # type: (str, str) -> Optional[str]
    """Fetch a topic's jsonStream file. Returns None on 404."""
    url = f"{BASE_URL}/{session_path}{stream_path}"
    r = _http.get(url, timeout=60)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.text
