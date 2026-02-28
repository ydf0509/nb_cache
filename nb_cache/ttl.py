# -*- coding: utf-8 -*-
"""TTL (time-to-live) parsing and utilities.

Supports:
  - int/float (seconds)
  - datetime.timedelta
  - str like "1h", "30m", "1h30m", "2d", "1d12h30m10s"
  - callable that returns any of the above
"""
import re
from datetime import timedelta

_TTL_PATTERN = re.compile(
    r'(?:(\d+)\s*d(?:ays?)?)?'
    r'\s*(?:(\d+)\s*h(?:ours?)?)?'
    r'\s*(?:(\d+)\s*m(?:in(?:utes?)?)?)?'
    r'\s*(?:(\d+)\s*s(?:ec(?:onds?)?)?)?',
    re.IGNORECASE,
)

_SENTINEL = object()


def ttl_to_seconds(ttl):
    """Convert various TTL representations to seconds (float).

    Returns None if ttl is None or 0.
    """
    if ttl is None:
        return None

    if callable(ttl) and not isinstance(ttl, (int, float)):
        ttl = ttl()

    if isinstance(ttl, (int, float)):
        return float(ttl) if ttl > 0 else None

    if isinstance(ttl, timedelta):
        return ttl.total_seconds() or None

    if isinstance(ttl, str):
        return _parse_ttl_string(ttl)

    raise TypeError("Unsupported TTL type: {!r}".format(type(ttl)))


def _parse_ttl_string(s):
    s = s.strip()
    if not s:
        return None

    try:
        return float(s)
    except (ValueError, TypeError):
        pass

    m = _TTL_PATTERN.fullmatch(s)
    if m is None:
        raise ValueError("Invalid TTL string: {!r}".format(s))

    days = int(m.group(1) or 0)
    hours = int(m.group(2) or 0)
    minutes = int(m.group(3) or 0)
    seconds = int(m.group(4) or 0)

    total = days * 86400 + hours * 3600 + minutes * 60 + seconds
    return float(total) if total > 0 else None
