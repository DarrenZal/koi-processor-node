"""Optional access to the KOI-net federation modules.

The Regen-scoped deployment of this service ships without the federation and
vault-sync modules. Code that emits federation events resolves them through
optional_federation_attr(), which returns the real function when the module is
installed and an async no-op when it is not.

Only the case where the requested module itself is missing counts as "no
federation". Any other import failure, including a missing dependency inside a
federation module that is present, propagates, so a broken federation install
on a full deployment is never silently swallowed.
"""
import importlib
import logging

logger = logging.getLogger(__name__)

_absent_logged = False


async def federation_unavailable(*_args, **_kwargs):
    """No-op stand-in used when the federation modules are not installed."""
    return None


def optional_federation_attr(module_name: str, attr: str):
    """Return ``module_name.attr``, or ``federation_unavailable`` when that module is absent."""
    global _absent_logged
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name != module_name:
            raise
        if not _absent_logged:
            logger.info(
                "Federation module %s is not installed; federation events are disabled in this deployment",
                module_name,
            )
            _absent_logged = True
        return federation_unavailable
    return getattr(module, attr)
