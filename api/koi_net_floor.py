"""Python floor for koi-net, imported before anything that imports koi_net at module level.

koi-net 2.1.2 uses PEP 695 `type` statements, so on Python < 3.12 importing it raises
SyntaxError, which `except ImportError` does not catch: the whole API failed to start.
This turns that into an ImportError naming the cause, so a node on an
older interpreter takes the existing "KOI-net federation not available" path, logged as an
error, instead of crashing. koi-net's metadata declares >= 3.10, so pip does not stop it.

It also quiets koi-net's structlog debug output (below).
"""
import sys

if sys.version_info < (3, 12):
    raise ImportError(
        "koi-net 2.1.2 needs Python >= 3.12 (PEP 695 type statements); "
        f"this interpreter is {sys.version.split()[0]}"
    )


# koi-net logs through structlog, and unconfigured structlog prints every level to stdout. On
# our path koi_net.protocol.secure.PrivateKey.sign logs, at debug, the public key, the message
# hash and the signature of every signature we make (none of it secret, all of it noise in a
# service log); koi-net's typed envelope classes would also log whole serialised envelopes,
# though we do not call them. Nothing of ours uses structlog, so below WARNING it is dropped
# here, once, unless the process configured structlog itself.
import logging as _logging

import structlog as _structlog

if not _structlog.is_configured():
    _structlog.configure(wrapper_class=_structlog.make_filtering_bound_logger(_logging.WARNING))
