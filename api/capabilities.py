"""Central capabilities registry for KOI runtime convergence.

Each deployment (personal, bkc_coordinator, bkc_leaf) enables a different
subset of features.  Capabilities can be set via environment variables or
by selecting a named profile.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _flag(var_name: str, default: bool = False) -> bool:
    """Read a boolean flag from an environment variable."""
    val = os.environ.get(var_name, "").strip().lower()
    if not val:
        return default
    return val in ("true", "1", "yes")


@dataclass(frozen=True)
class Capabilities:
    web_sensor: bool = False
    github_sensor: bool = False
    mediawiki_sensor: bool = False
    llm_enrichment: bool = False
    vault_sync: bool = False
    terminusdb: bool = False
    pipeline: bool = False
    graph_queries: bool = True          # pure SQL, no deps — always on
    coordinator_endpoints: bool = False
    assertion_history: bool = True      # ADR-001 core invariant
    query_endpoint: bool = False        # dynamic SQL query endpoint (personal only)

    deployment_profile: str = "personal"

    # -- constructors --------------------------------------------------------

    @classmethod
    def from_env(cls) -> Capabilities:
        """Build capabilities from environment variables.

        Named deployment profiles provide the baseline capability set.
        Individual env vars can then override specific flags.
        """
        profile = (
            os.environ.get("DEPLOYMENT_PROFILE")
            or os.environ.get("KOI_MODE")
            or "personal"
        ).strip()
        base = cls.from_profile(profile)
        return cls(
            web_sensor=_flag("WEB_SENSOR_ENABLED", default=base.web_sensor),
            github_sensor=_flag("GITHUB_SENSOR_ENABLED", default=base.github_sensor),
            mediawiki_sensor=_flag("MEDIAWIKI_SENSOR_ENABLED", default=base.mediawiki_sensor),
            llm_enrichment=_flag("LLM_ENRICHMENT_ENABLED", default=base.llm_enrichment),
            vault_sync=_flag("VAULT_SYNC_ENABLED", default=base.vault_sync),
            terminusdb=_flag("TERMINUSDB_ENABLED", default=base.terminusdb),
            pipeline=_flag("PIPELINE_ENABLED", default=base.pipeline),
            graph_queries=_flag("GRAPH_QUERIES_ENABLED", default=base.graph_queries),
            coordinator_endpoints=_flag("COORDINATOR_ENDPOINTS_ENABLED", default=base.coordinator_endpoints),
            assertion_history=_flag("ASSERTION_HISTORY_ENABLED", default=base.assertion_history),
            query_endpoint=_flag("QUERY_ENDPOINT_ENABLED", default=base.query_endpoint),
            deployment_profile=profile,
        )

    @classmethod
    def from_profile(cls, profile: str) -> Capabilities:
        """Return sensible defaults for a named deployment profile."""
        profiles = {
            "personal": cls(
                web_sensor=True,
                vault_sync=True,
                terminusdb=False,
                graph_queries=True,
                assertion_history=True,
                query_endpoint=True,
                deployment_profile="personal",
            ),
            "bkc_coordinator": cls(
                web_sensor=True,
                github_sensor=True,
                mediawiki_sensor=True,
                llm_enrichment=True,
                pipeline=True,
                coordinator_endpoints=True,
                graph_queries=True,
                assertion_history=True,
                deployment_profile="bkc_coordinator",
            ),
            "bkc_leaf": cls(
                graph_queries=True,
                assertion_history=True,
                deployment_profile="bkc_leaf",
            ),
        }
        if profile not in profiles:
            raise ValueError(
                f"Unknown profile {profile!r}. "
                f"Choose from: {', '.join(sorted(profiles))}"
            )
        return profiles[profile]
