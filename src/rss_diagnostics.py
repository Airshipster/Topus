"""Small sanitized response fingerprints; never log a URL, body or credentials."""
from collections.abc import Mapping
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import math
import re


def http_failure_evidence(response, *, direct, now=None):
    raw = response.content
    raw = bytes(raw[:4096]) if isinstance(raw, (bytes, bytearray)) else b''
    lower = raw.lower()
    if not raw:
        kind = 'empty-or-unavailable'
    elif any(marker in lower for marker in (b'unusual traffic', b'captcha', b'sign in to confirm', b'/sorry/')):
        kind = 'access-challenge'
    elif b'quotaexceeded' in lower or b'dailylimitexceeded' in lower or b'resource_exhausted' in lower:
        kind = 'quota-error'
    elif b'404' in lower and (b'was not found on this server' in lower or b'that\xe2\x80\x99s an error' in lower):
        kind = 'not-found-page'
    elif b'<html' in lower or b'<!doctype html' in lower:
        kind = 'html'
    elif b'http://www.w3.org/2005/atom' in lower:
        kind = 'atom'
    else:
        kind = 'other'
    headers = response.headers if isinstance(response.headers, Mapping) else {}
    content_type = str(headers.get('Content-Type', '')).split(';', 1)[0].strip().lower()
    if not re.fullmatch(r'[a-z0-9.+-]+/[a-z0-9.+-]+', content_type):
        content_type = 'unknown'
    retry_after = None
    raw_retry = headers.get('Retry-After')
    if isinstance(raw_retry, str):
        try:
            retry_after = float(raw_retry) if re.fullmatch(r'\d+(?:\.\d+)?', raw_retry) else (
                parsedate_to_datetime(raw_retry).timestamp()
                - (now if now is not None else datetime.now(timezone.utc).timestamp()))
            if not math.isfinite(retry_after) or retry_after < 0:
                retry_after = None
        except (ValueError, TypeError, OverflowError):
            pass
    return {'transport': 'direct' if direct else 'worker', 'status': int(response.status_code),
            'contentType': content_type, 'bodyKind': kind, 'sampleBytes': len(raw),
            'sampleSha256': hashlib.sha256(raw).hexdigest(), 'retryAfterSeconds': retry_after}


def log_http_failure(response, *, direct):
    try:
        print('RSS_HTTP_EVIDENCE ' + json.dumps(http_failure_evidence(response, direct=direct),
                                               sort_keys=True), flush=True)
    except Exception:
        # Diagnostics must never change the feed outcome or retry behavior.
        return
