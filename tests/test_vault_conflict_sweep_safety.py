"""scripts/vault_conflict_sweep.py deletes and flags only with --apply, and has no default
vault (D13).

Offline: the vault and the backup root are temporary directories, and the review-task
call is replaced by a recorder.
"""

from __future__ import annotations

import importlib

import pytest


@pytest.fixture
def sweep(monkeypatch, tmp_path):
    mod = importlib.import_module("scripts.vault_conflict_sweep")
    flagged: list[str] = []
    monkeypatch.setattr(mod, "flag_for_review", lambda cpath, *a, **k: flagged.append(cpath) or True)
    monkeypatch.setattr(mod, "BACKUP_ROOT", str(tmp_path / "backups"))
    vault = tmp_path / "vault"
    (vault / "Meetings").mkdir(parents=True)
    note = "---\nlast_synced: 1\n---\nSame body.\n"
    (vault / "Meetings" / "Note.md").write_text(note)
    safe = vault / "Meetings" / "Note (conflict 2026-08-25 07-00-00).md"
    safe.write_text("---\nlast_synced: 0\n---\nSame body.\n")
    review = vault / "Meetings" / "Other (conflict 2026-08-25 07-00-00).md"
    review.write_text("Only here.\n")
    (vault / "Meetings" / "Other.md").write_text("Live.\n")
    return mod, vault, safe, review, flagged, tmp_path / "backups"


def test_help(sweep):
    mod, *_ = sweep
    with pytest.raises(SystemExit) as exc:
        mod.main(["--help"])
    assert exc.value.code == 0


def test_no_vault_named_is_refused(sweep, monkeypatch):
    mod, _, safe, review, flagged, backups = sweep
    monkeypatch.setattr(mod, "VAULT_PATH", "")
    assert mod.main([]) == 2
    assert mod.main(["--apply"]) == 2
    assert safe.exists() and flagged == [] and not backups.exists()


@pytest.mark.parametrize("flags", [[], ["--dry-run"], ["--dry-run", "--apply"]])
def test_without_apply_nothing_changes(sweep, flags):
    mod, vault, safe, review, flagged, backups = sweep
    assert mod.main(["--vault", str(vault), *flags]) == 0
    assert safe.exists() and review.exists()
    assert flagged == []
    assert not backups.exists()


def test_vault_from_environment_variable(sweep, monkeypatch):
    mod, vault, safe, _, flagged, _ = sweep
    monkeypatch.setattr(mod, "VAULT_PATH", str(vault))
    assert mod.main([]) == 0
    assert safe.exists() and flagged == []


def test_apply_backs_up_deletes_and_flags(sweep):
    mod, vault, safe, review, flagged, backups = sweep
    assert mod.main(["--vault", str(vault), "--apply"]) == 0
    assert not safe.exists()
    assert review.exists()
    assert flagged == [str(review)]
    backed_up = list(backups.rglob("Note (conflict 2026-08-25 07-00-00).md"))
    assert len(backed_up) == 1
