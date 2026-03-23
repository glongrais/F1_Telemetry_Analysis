import base64
import json
import logging
import re
import zlib
from typing import List, Tuple

logger = logging.getLogger(__name__)

_LINE_RE = re.compile(r"^(\d+):(\d+):(\d+\.?\d*)(.*)")


def _parse_timestamp(h, m, s):
    # type: (str, str, str) -> float
    return int(h) * 3600 + int(m) * 60 + float(s)


def parse_jsonstream(text):
    # type: (str) -> List[Tuple[float, dict]]
    """Parse a .jsonStream file into (timestamp_seconds, payload) tuples."""
    results = []
    for line in text.strip().split("\r\n"):
        if not line:
            continue
        m = _LINE_RE.match(line)
        if not m:
            continue
        ts = _parse_timestamp(m.group(1), m.group(2), m.group(3))
        raw = m.group(4)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, ValueError) as e:
            logger.debug("Skipping malformed JSON at timestamp %.3f: %s", ts, e)
            continue
        results.append((ts, payload))
    return results


def decompress_payload(b64_data):
    # type: (str) -> str
    """Decompress a base64-encoded raw deflate payload."""
    raw = base64.b64decode(b64_data)
    return zlib.decompress(raw, -zlib.MAX_WBITS).decode("utf-8-sig")


def parse_compressed_jsonstream(text):
    # type: (str) -> List[Tuple[float, dict]]
    """Parse a .z compressed jsonStream file."""
    results = []
    for line in text.strip().split("\r\n"):
        if not line:
            continue
        m = _LINE_RE.match(line)
        if not m:
            continue
        ts = _parse_timestamp(m.group(1), m.group(2), m.group(3))
        b64_payload = m.group(4)
        if not b64_payload:
            continue
        try:
            decompressed = decompress_payload(b64_payload)
            payload = json.loads(decompressed)
        except Exception as e:
            logger.debug("Skipping malformed compressed entry at timestamp %.3f: %s", ts, e)
            continue
        results.append((ts, payload))
    return results
