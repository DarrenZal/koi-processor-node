"""
OntologyLoader: future hook for dynamic ontology loading from Fuseki or local files.

NOT WIRED INTO STARTUP OR POSTPROCESSING in this phase.

This module becomes valuable when:
- The ontology grows and manual alias maintenance becomes burdensome
- Someone adds explicit cross-ontology mapping triples (owl:equivalentClass, skos:exactMatch)
- Dynamic ontology reloading is needed

Currently, hardcoded aliases in entity_types.py and ontology_normalizer_module.py
are simpler and sufficient for the ~12 cross-ontology type mappings.
"""

import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# RDS → KOI hardcoded alias map (same as entity_types.py, for future loader bootstrap)
_HARDCODED_RDS_TO_KOI = {
    "Agent": "AGENT",
    "WorkOrder": "WORK_ORDER",
    "ProjectInfo": "PROJECT",
    "CreditClassInfo": "CREDIT_CLASS",
    "CarbonCreditClassInfo": "CREDIT_CLASS",
    "GovernanceDecision": "GOVERNANCE_PROPOSAL",
    "VoiceCouncilSession": "EVENT",
    "CoherenceCheck": "PROCESS",
    "GovernanceProcess": "PROCESS",
    "GovernanceStage": "PROCESS",
    "Individual": "PERSON",
    "Organization": "ORGANIZATION",
}


class OntologyLoader:
    """
    Loads RDF ontology from Fuseki named graph or local TTL file.
    Builds type hierarchy index from rdfs:subClassOf triples.
    """

    def __init__(self):
        self._type_hierarchy: Dict[str, List[str]] = {}  # child -> [parents]
        self._triple_count: int = 0
        self._is_loaded: bool = False

    @property
    def is_loaded(self) -> bool:
        return self._is_loaded

    @property
    def triple_count(self) -> int:
        return self._triple_count

    def load_from_file(self, filepath: str) -> bool:
        """
        Load ontology from a local TTL file.

        Returns True on success, False on failure.
        """
        try:
            from rdflib import Graph, RDFS
            g = Graph()
            g.parse(filepath, format="turtle")
            self._triple_count = len(g)
            self._build_hierarchy(g)
            self._is_loaded = True
            logger.info(f"Loaded ontology from {filepath}: {self._triple_count} triples")
            return True
        except Exception as e:
            logger.warning(f"Failed to load ontology from {filepath}: {e}")
            return False

    def load_from_fuseki(self, fuseki_url: str, dataset: str, graph_uri: str,
                         user: Optional[str] = None, password: Optional[str] = None) -> bool:
        """
        Load ontology from a Fuseki named graph via SPARQL CONSTRUCT.

        Returns True on success, False on failure (graceful — no exception raised).
        """
        try:
            from rdflib import Graph, RDFS
            import requests

            endpoint = f"{fuseki_url}/{dataset}/sparql"
            query = f"CONSTRUCT {{ ?s ?p ?o }} WHERE {{ GRAPH <{graph_uri}> {{ ?s ?p ?o }} }}"

            auth = None
            if user and password:
                auth = (user, password)

            resp = requests.post(
                endpoint,
                data={"query": query},
                headers={"Accept": "text/turtle"},
                auth=auth,
                timeout=30,
            )
            resp.raise_for_status()

            g = Graph()
            g.parse(data=resp.text, format="turtle")
            self._triple_count = len(g)
            self._build_hierarchy(g)
            self._is_loaded = True
            logger.info(f"Loaded ontology from Fuseki {graph_uri}: {self._triple_count} triples")
            return True
        except Exception as e:
            logger.warning(f"Failed to load ontology from Fuseki: {e}")
            return False

    def _build_hierarchy(self, graph) -> None:
        """Extract rdfs:subClassOf hierarchy from the graph."""
        from rdflib import RDFS
        self._type_hierarchy = {}
        for s, p, o in graph.triples((None, RDFS.subClassOf, None)):
            child = self._local_name(str(s))
            parent = self._local_name(str(o))
            if child and parent:
                self._type_hierarchy.setdefault(child, []).append(parent)

    def _local_name(self, uri: str) -> Optional[str]:
        """Extract local name from a URI (after # or last /)."""
        if "#" in uri:
            return uri.split("#")[-1]
        if "/" in uri:
            return uri.rsplit("/", 1)[-1]
        return uri

    def get_parent_types(self, type_name: str) -> List[str]:
        """Get direct parent types from rdfs:subClassOf hierarchy."""
        return self._type_hierarchy.get(type_name, [])

    def get_koi_type(self, rds_type: str) -> Optional[str]:
        """
        Map an rds type name to a KOI canonical type.

        Currently returns from hardcoded map. When cross-ontology mapping
        triples exist in the ontology, this can be populated dynamically.
        """
        return _HARDCODED_RDS_TO_KOI.get(rds_type)

    def get_type_aliases(self) -> Dict[str, str]:
        """
        Return the full rds→KOI alias map.

        Currently hardcoded. Future: populated from ontology triples.
        """
        return dict(_HARDCODED_RDS_TO_KOI)
