import json

import pytest

from scripts.backfill_meeting_attendance import write_private_json
from scripts.repair_historical_meeting_mappings import (
    RepairError,
    choose_representative,
    classify_mapping_rows,
    compute_plan_digest,
    leading_meeting_date,
    load_plan,
    require_confirmation,
)


def _mapping(mapping_id, name, uri="orn:meeting:old"):
    return {
        "id": mapping_id,
        "vault_rid": f"orn:vault:{mapping_id}",
        "vault_path": f"Meetings/{name}.md",
        "canonical_uri": uri,
        "entity_type": "Meeting",
        "name": name,
        "content_hash": str(mapping_id),
        "sync_status": "linked",
        "last_synced": None,
        "created_at": None,
        "visibility_scope": "public",
    }


def _plan():
    plan = {
        "schema_version": 1,
        "generated_at": "now",
        "snapshot_digest": "snapshot",
        "mutation_set_digest": "mutation",
        "moves": [],
    }
    plan["plan_digest"] = compute_plan_digest(plan)
    return plan


def test_leading_date_requires_full_iso_date_at_start():
    assert leading_meeting_date("2026-01-13 Team Meeting") == "2026-01-13"
    assert leading_meeting_date("Cascadia Sync Sept 24") is None
    assert leading_meeting_date("Notes for 2026-01-13 Team Meeting") is None


def test_classification_moves_only_non_anchor_date_partitions():
    rows = [
        _mapping(1, "2025-11-13 Landscape Meeting"),
        _mapping(2, "2025-11-13 Landscape Meeting Transcript"),
        _mapping(3, "2026-01-13 Landscape Meeting (Quinn)"),
        _mapping(4, "2026-01-13 Landscape Meeting"),
        _mapping(5, "Dateless Singleton", "orn:meeting:dateless"),
    ]
    result = classify_mapping_rows(
        rows,
        {
            "orn:meeting:old": "2025-11-13 Landscape Meeting",
            "orn:meeting:dateless": "Dateless Singleton",
        },
    )
    assert [row["id"] for row in result["moves"]] == [3, 4]
    assert {row["expected_canonical_uri"] for row in result["moves"]} == {
        "orn:personal-koi.entity:meeting-2026-01-13-landscape-meeting-0cc9dfa6b14b"
    }
    assert result["moves"][0]["representative_name"] == "2026-01-13 Landscape Meeting"
    assert [row["id"] for row in result["dateless_singletons"]] == [5]


def test_representative_prefers_primary_note_over_shorter_transcript_or_parenthetical():
    rows = [
        _mapping(1, "2026-01-13 Sync Transcript"),
        _mapping(2, "2026-01-13 Sync (Quinn)"),
        _mapping(3, "2026-01-13 Sync Meeting"),
    ]
    assert choose_representative(rows)["id"] == 3


def test_dateless_row_in_collapsed_group_aborts_for_manual_review():
    rows = [
        _mapping(1, "2026-01-13 Team Meeting"),
        _mapping(2, "Team Meeting without date"),
    ]
    with pytest.raises(RepairError, match="Dateless Meeting mapping"):
        classify_mapping_rows(rows, {"orn:meeting:old": "2026-01-13 Team Meeting"})


def test_apply_and_rollback_require_literal_confirmation():
    with pytest.raises(RepairError, match="--apply"):
        require_confirmation(False, "apply")
    with pytest.raises(RepairError, match="--rollback"):
        require_confirmation(False, "rollback")


def test_tampered_plan_is_rejected(tmp_path):
    path = tmp_path / "plan.json"
    write_private_json(_plan(), path)
    value = json.loads(path.read_text(encoding="utf-8"))
    value["moves"].append({"id": 1})
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(RepairError, match="plan digest mismatch"):
        load_plan(path)
