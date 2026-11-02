"""Reject credential-shaped facts at the write path, loudly.

WHY THIS EXISTS
---------------
The extraction pipeline once stored real credentials in knowledge_facts as ordinary
facts (HAS_PASSWORD, HAS_TOKEN, SSH_USERNAME), and the API served them until they
were retracted.

None of those four predicates appears in any vocabulary. A generated closed
predicate list (the ontology work) would have prevented the class at the source;
this guard is the boundary check until that lands, and remains useful after,
because it also inspects VALUES.

CALIBRATED AGAINST REAL DATA, NOT INVENTED CONTROLS
---------------------------------------------------
Measured 2026-09-02 against live knowledge_facts:

  positive control  9 real credential facts (retracted rows survive with
                    valid_to set, so the actual values were available)
                    -> 8/9 caught
  negative control  1,233 legitimate no-whitespace literals >=16 chars
                    (URLs, filesystem paths, RIDs, slugs, hashes)
                    -> 1 false positive (0.081%)

Two things that calibration caught which reading the code would not have:

1. The three real HAS_TOKEN values are 32-char *lowercase hex* -- NOT mixed
   case+digits. A "high entropy AND mixed character classes" test, which is the
   obvious thing to write, MISSES all three. An invented positive control would
   have passed while the real secrets walked through.

2. `\bTOKEN\b` never matches HAS_TOKEN, because '_' is a word character so there
   is no word boundary. An early version of this pattern was silently inert on
   the exact real-world case. The patterns below use plain substrings.

The value rule earned its place independently: it found USES_TOKEN on
project-node-2 -- a 32-char hex token, still current, same date and same subject
URI as a retracted HAS_TOKEN row, missed by the predicate-name cleanup.

KNOWN GAP, deliberately not closed here: SSH_USERNAME is not caught. A 10-char
username has no value-shape signal and USERNAME is not in the operator-specified
predicate pattern. Widening it is an operator decision, not this module's.
"""

from __future__ import annotations

import re
from typing import Optional, Tuple

# Operator-specified predicate pattern. PLAIN SUBSTRINGS -- see note 2 above.
# These are refused unconditionally: a fact asserting a password/token/key is
# wrong whether or not it carries a literal, because the value may sit in
# fact_text instead of object_literal.
SECRET_PREDICATE = re.compile(
    r"(PASSWORD|PASSWD|TOKEN|SECRET|API_?KEY|APIKEY|ACCESS_?KEY|CREDENTIAL|PRIVATE_?KEY)",
    re.IGNORECASE,
)

# USERNAME is a SEPARATE, WEAKER tier, and the reason is worth stating because
# the next person will ask why it isn't just appended to the pattern above.
#
# WHY PATTERN-ONLY IS THE ONLY MECHANISM THAT CAN WORK HERE: the real
# SSH_USERNAME fact was a 10-character lowercase string. It has no entropy, no
# distinguishing shape, and is indistinguishable from ordinary prose by any
# value-based test. No entropy threshold, length rule, or character-class check
# will ever catch it. The predicate name is the ONLY available signal -- so if
# the predicate rule does not catch a username, nothing downstream will.
#
# WHY IT IS CONDITIONAL: measured against the live corpus, USERNAME predicates
# split into two kinds. Ones carrying a literal (HAS_USERNAME, 3 current facts
# of 19/11/5 chars) are account handles -- half of a credential pair, correctly
# refused. Ones with an ENTITY object and object_literal IS NULL
# (SENT_GITHUB_USERNAME_TO, USERNAME_FOR) assert a relationship and carry no
# value at all; there is nothing to leak, and refusing them is a false positive.
USERNAME_PREDICATE = re.compile(r"(USERNAME|USER_?NAME|LOGIN_?NAME)", re.IGNORECASE)

# Predicates whose values are legitimately long hex. Exempt from the hex rule
# ONLY -- the predicate rule above still applies to them if it matches.
DIGEST_PREDICATE = re.compile(r"(HASH|DIGEST|CHECKSUM|SHA\d*|COMMIT|FINGERPRINT)", re.IGNORECASE)

# Shapes that are legitimate in this corpus and must never be flagged. Derived
# from the actual negative-control population, not guessed: it is dominated by
# URLs and filesystem paths.
SAFE_SHAPE = re.compile(
    r"""^(
        [a-z][a-z0-9+.-]*://
      | [~/]
      | orn: | urn: | did:
      | [0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$
      | \d{1,3}(\.\d{1,3}){3}$
      | [\d.:+-]+$
    )""",
    re.IGNORECASE | re.VERBOSE,
)

VENDOR_TOKEN = re.compile(
    r"(sk-[A-Za-z0-9]{16,}"
    r"|ghp_[A-Za-z0-9]{20,}"
    r"|xox[baprs]-[A-Za-z0-9-]{10,}"
    r"|AKIA[0-9A-Z]{12,}"
    r"|eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}"
    r"|-----BEGIN [A-Z ]*PRIVATE KEY-----)"
)

HEX_SECRET = re.compile(r"^[0-9a-fA-F]{32,}$")

_MIN_LEN, _MAX_LEN = 16, 200


def check_fact(predicate: Optional[str], object_literal: Optional[str]) -> Tuple[bool, str]:
    """Return (is_credential_shaped, human-readable reason).

    The reason NEVER contains the value -- it is destined for a 422 body and
    application logs, which is exactly where a secret must not be echoed.
    """
    if predicate and SECRET_PREDICATE.search(predicate):
        return True, (
            f"predicate {predicate!r} matches the credential pattern "
            f"(PASSWORD|TOKEN|SECRET|API_KEY|CREDENTIAL|PRIVATE_KEY). "
            f"Credentials must not be stored as facts."
        )

    # Weaker tier: refuse a username only when it actually carries a value.
    # A relational username fact with an entity object has nothing to leak.
    if predicate and USERNAME_PREDICATE.search(predicate) and (object_literal or "").strip():
        return True, (
            f"predicate {predicate!r} names a username and carries a literal value. "
            f"Account identifiers are half of a credential pair and must not be "
            f"stored as facts. (A relational form with an entity object is allowed.)"
        )

    value = object_literal or ""
    if VENDOR_TOKEN.search(value):
        return True, "object_literal carries a recognised vendor credential prefix"

    if not value or re.search(r"\s", value) or SAFE_SHAPE.match(value):
        return False, ""
    if not (_MIN_LEN <= len(value) <= _MAX_LEN):
        return False, ""

    if HEX_SECRET.match(value) and not (predicate and DIGEST_PREDICATE.search(predicate)):
        return True, (
            f"object_literal is a {len(value)}-character hex string, the shape of a "
            f"key or token. If this is a digest, name the predicate so it says so "
            f"(…HASH/…DIGEST/…CHECKSUM/…COMMIT)."
        )

    return False, ""
