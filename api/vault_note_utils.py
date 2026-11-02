"""Shared vault note utilities for web ingest and backfill scripts.

Pure functions — no web framework or DB dependencies.
"""

import hashlib
import os
import re
import unicodedata
from typing import Optional

import yaml


def sanitize_filename(name: str) -> Optional[str]:
    """Remove characters unsafe for filenames. Returns None if name sanitizes to empty."""
    s = re.sub(r'[/\\\x00]', '', name)
    s = s.strip('. ')
    s = re.sub(r'\.{2,}', '.', s)
    s = s[:200]
    if not s:
        return None
    return s


def vault_slug(name: str) -> str:
    """Convert entity name to vault RID slug (lowercase, hyphenated, ASCII-safe).

    Falls back to hash suffix if slug would be empty (pure Unicode names).
    """
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    s = s.strip()
    s = re.sub(r"[\s_]+", "-", s)
    s = re.sub(r"[^\w-]", "", s)
    s = re.sub(r"-{2,}", "-", s)
    s = s.strip("-").lower()
    if not s:
        s = hashlib.sha256(name.encode()).hexdigest()[:12]
    return s


def build_frontmatter(data: dict) -> str:
    """Serialize frontmatter dict to YAML with proper escaping."""
    return '---\n' + yaml.dump(
        data, default_flow_style=False, allow_unicode=True, sort_keys=False
    ) + '---\n'


def vault_note_path(vault_root: str, folder: str, safe_name: str) -> Optional[str]:
    """Compute absolute note path with path traversal guard.

    Returns None if the resulting path escapes the vault root.
    """
    note_dir = os.path.join(vault_root, folder)
    note_path = os.path.join(note_dir, f"{safe_name}.md")
    note_real = os.path.realpath(note_path)
    vault_real = os.path.realpath(vault_root)
    try:
        if os.path.commonpath([note_real, vault_real]) != vault_real:
            return None
    except ValueError:
        return None
    return note_path
