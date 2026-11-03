"""
Tests for regen-data-standards type normalization in KOI.

WS3: Verifies that rds type names, prefixed types, and full URIs
all normalize correctly to KOI canonical types.
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.core.entity_types import normalize_type


class TestRdsTypeNormalization:
    """Test regen-data-standards types normalize to KOI canonical types."""

    # --- rfs: prefix stripping ---

    def test_rfs_agent(self):
        assert normalize_type("rfs:Agent") == "AGENT"

    def test_rfs_work_order(self):
        assert normalize_type("rfs:WorkOrder") == "WORK_ORDER"

    def test_rfs_credit_class_info(self):
        assert normalize_type("rfs:CreditClassInfo") == "CREDIT_CLASS"

    def test_rfs_project_info(self):
        assert normalize_type("rfs:ProjectInfo") == "PROJECT"

    def test_rfs_individual(self):
        assert normalize_type("rfs:Individual") == "PERSON"

    def test_rfs_organization(self):
        assert normalize_type("rfs:Organization") == "ORGANIZATION"

    # --- Full URI stripping ---

    def test_full_uri_agent(self):
        assert normalize_type("https://framework.regen.network/schema/Agent") == "AGENT"

    def test_full_uri_project_info(self):
        assert normalize_type("https://framework.regen.network/schema/ProjectInfo") == "PROJECT"

    def test_full_uri_work_order(self):
        assert normalize_type("https://framework.regen.network/schema/WorkOrder") == "WORK_ORDER"

    def test_full_uri_governance_decision(self):
        assert normalize_type("https://framework.regen.network/schema/GovernanceDecision") == "GOVERNANCE_PROPOSAL"

    def test_full_uri_voice_council_session(self):
        assert normalize_type("https://framework.regen.network/schema/VoiceCouncilSession") == "EVENT"

    def test_full_uri_coherence_check(self):
        assert normalize_type("https://framework.regen.network/schema/CoherenceCheck") == "PROCESS"

    # --- Bare type names (no prefix) ---

    def test_bare_agent(self):
        assert normalize_type("AGENT") == "AGENT"

    def test_bare_work_order(self):
        assert normalize_type("WORK_ORDER") == "WORK_ORDER"

    def test_bare_workorder(self):
        assert normalize_type("WorkOrder") == "WORK_ORDER"

    def test_bare_governance_decision(self):
        assert normalize_type("GovernanceDecision") == "GOVERNANCE_PROPOSAL"

    def test_bare_carbon_credit_class_info(self):
        assert normalize_type("CarbonCreditClassInfo") == "CREDIT_CLASS"

    # --- Existing types unchanged (regression) ---

    def test_person_unchanged(self):
        assert normalize_type("PERSON") == "PERSON"

    def test_organization_unchanged(self):
        assert normalize_type("ORGANIZATION") == "ORGANIZATION"

    def test_project_unchanged(self):
        assert normalize_type("PROJECT") == "PROJECT"

    def test_event_unchanged(self):
        assert normalize_type("EVENT") == "EVENT"

    def test_credit_class_unchanged(self):
        assert normalize_type("CREDIT_CLASS") == "CREDIT_CLASS"

    def test_governance_proposal_unchanged(self):
        assert normalize_type("GOVERNANCE_PROPOSAL") == "GOVERNANCE_PROPOSAL"

    # --- koi# prefix (existing behavior) ---

    def test_koi_person(self):
        assert normalize_type("koi#PERSON") == "PERSON"

    def test_koi_agent(self):
        assert normalize_type("koi#AGENT") == "AGENT"

    # --- Edge cases ---

    def test_none_returns_entity(self):
        assert normalize_type(None) == "ENTITY"

    def test_empty_returns_entity(self):
        assert normalize_type("") == "ENTITY"

    def test_unknown_returns_entity(self):
        assert normalize_type("COMPLETELY_UNKNOWN_TYPE") == "ENTITY"
