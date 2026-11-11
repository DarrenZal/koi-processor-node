#!/usr/bin/env python3
"""Delivery classification and dead-letter handling for the coordinator ->
event-bridge forwarder.

Kept separate from the forwarder script so the decision logic can be tested
offline with no network, no dotenv and no signing key. The forwarder stays a
thin loop over these pure functions.

Policy (see the A5-A9 plan, PR2):
- A 200 with ``success: true`` in the body is CONFIRM: the bridge accepted it.
- A 400 or 422 is a permanent client error: DEAD_LETTER (append to a JSONL file,
  then confirm so the coordinator does not keep redelivering it).
- A connection error (no status), a 401 or 403, or any 5xx is transient: RETRY.
  The forwarder keeps the batch and retries with backoff WITHOUT polling again,
  logging CRITICAL, so an outage or a missing key never silently drops events.
- Anything else, including a 200 with ``success: false``, is treated as RETRY,
  erring toward not losing events.
"""

from __future__ import annotations

import json
import os
import time
from enum import Enum
from typing import Any, Optional


class Disposition(str, Enum):
    CONFIRM = "confirm"
    DEAD_LETTER = "dead_letter"
    RETRY = "retry"


DEAD_LETTER_ENV = "KOI_FORWARDER_DEAD_LETTER"
# Default to a service-private directory under the running user's home, never a
# world-writable location like /tmp (where another user could pre-create or
# symlink the predictable filename). Deploys may override via the env, but the
# target directory must be private - see _ensure_private_dir.
_DEFAULT_DEAD_LETTER = os.path.join(
    os.path.expanduser("~"), ".koi-forwarder", "deadletter.jsonl"
)


def classify_delivery(status_code: Optional[int], body_success: Optional[bool]) -> Disposition:
    """Classify one bridge delivery outcome.

    ``status_code`` is None for a connection error. ``body_success`` is the
    ``success`` field of a JSON body when present (only trusted on a 200).
    """
    if status_code is None:
        return Disposition.RETRY
    if status_code == 200:
        return Disposition.CONFIRM if body_success else Disposition.RETRY
    if status_code in (400, 422):
        return Disposition.DEAD_LETTER
    if status_code in (401, 403):
        return Disposition.RETRY
    if 500 <= status_code <= 599:
        return Disposition.RETRY
    return Disposition.RETRY


def dead_letter_path() -> str:
    return os.getenv(DEAD_LETTER_ENV) or _DEFAULT_DEAD_LETTER


def _ensure_private_dir(directory: str) -> None:
    """Ensure the dead-letter directory is private before writing into it.

    Dead-letter records contain document contents, so they must never be written
    into a group- or world-WRITABLE directory, where another user could
    pre-create or symlink the (predictable) file. Create the directory 0700 if it
    is missing; refuse if an existing directory is group/world-writable or not
    owned by this user.
    """
    if not directory:
        return
    if not os.path.isdir(directory):
        os.makedirs(directory, mode=0o700, exist_ok=True)
        os.chmod(directory, 0o700)  # clamp against a relaxed umask
        return
    st = os.stat(directory)
    if (st.st_mode & 0o022) or (st.st_uid != os.getuid()):
        raise PermissionError(
            "dead-letter directory is not private (group/world-writable or not "
            f"owned by this user); refusing to write: {directory}"
        )


def write_dead_letter(
    event: Any,
    event_id: Optional[str],
    reason: str,
    path: Optional[str] = None,
) -> str:
    """Append one dead-lettered event to the JSONL file and return the path.

    Never raises on a serialisation problem (an un-serialisable event is stored
    as its repr so the loop can still confirm and move on), but DOES raise if the
    target directory is not private, so document contents are never written to a
    shared location.
    """
    target = path or dead_letter_path()
    try:
        payload = json.dumps(event, default=str)
    except Exception:
        payload = json.dumps(repr(event))
    record = {
        "ts": time.time(),
        "event_id": event_id,
        "reason": reason,
        "event": json.loads(payload) if payload and payload[0] in "[{\"" else payload,
    }
    _ensure_private_dir(os.path.dirname(target))
    # O_NOFOLLOW refuses a symlink at the final path component; combined with the
    # private parent directory, another user cannot pre-create or redirect the
    # file. 0600 applies on create; fchmod clamps a file left by a prior run.
    flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(target, flags, 0o600)
    with os.fdopen(fd, "a", encoding="utf-8") as handle:
        try:
            os.fchmod(handle.fileno(), 0o600)
        except OSError:
            pass
        handle.write(json.dumps(record, default=str) + "\n")
    return target


def backoff_delay(attempt: int, base: float = 1.0, cap: float = 60.0) -> float:
    """Exponential backoff with a cap. ``attempt`` is 0-based."""
    if attempt < 0:
        attempt = 0
    return min(cap, base * (2 ** attempt))


MAX_RETRIES_ENV = "KOI_FORWARDER_MAX_RETRIES"
_DEFAULT_MAX_RETRIES = 8


def max_retries() -> int:
    """Number of transient-retry rounds before a stuck batch is dead-lettered
    instead of held forever. Bounds the hold so one persistent condition (a wrong
    key, a deterministic 500) cannot wedge the whole pipeline. 0 dead-letters on
    the first transient failure. Default 8 (~3 min of holding with the backoff cap)."""
    raw = os.getenv(MAX_RETRIES_ENV)
    if raw is None:
        return _DEFAULT_MAX_RETRIES
    try:
        return max(0, int(raw))
    except ValueError:
        return _DEFAULT_MAX_RETRIES
