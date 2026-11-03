#!/usr/bin/env python3
"""
Regenerate Fuseki Graph from Entity Registry and koi_relationships

This script:
1. Exports entity_registry (PostgreSQL) to RDF (Turtle format)
2. Exports koi_relationships to RDF triples
3. Clears target Fuseki dataset
4. Loads new RDF graph into Fuseki
5. Validates triple count

Conformance:
- HTTPS everywhere (no HTTP URIs)
- Types use koi# namespace with UPPERCASE names (no "Entity" suffix)
- Predicates use koi# namespace with lowercase snake_case
- Staging mode with safety latch for production

Requirements:
- rdflib
- SPARQLWrapper
- psycopg2
- requests
"""

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from rdflib import Graph, Namespace, URIRef, Literal, RDF, RDFS
from SPARQLWrapper import SPARQLWrapper, POST

# ============================================================================
# ENVIRONMENT CONFIGURATION - Read from environment variables
# ============================================================================
STAGING = os.environ.get('KOI_STAGING', 'false').lower() == 'true'
FUSEKI_PROD_ENDPOINT = os.environ.get('FUSEKI_ENDPOINT', 'http://localhost:3030/koi')
FUSEKI_STAGING_ENDPOINT = os.environ.get('FUSEKI_STAGING_ENDPOINT', 'http://localhost:3030/koi-staging')
ENDPOINT = FUSEKI_STAGING_ENDPOINT if STAGING else FUSEKI_PROD_ENDPOINT
FUSEKI_USER = os.environ.get('FUSEKI_USER', 'admin')
FUSEKI_PASSWORD = os.environ.get('FUSEKI_PASSWORD', 'admin')

# ============================================================================
# NAMESPACES - Conformance: HTTPS everywhere
# ============================================================================
KOI = Namespace("https://regen.network/koi#")  # Types and predicates
PROV = Namespace("http://www.w3.org/ns/prov#")
SCHEMA = Namespace("http://schema.org/")


def get_type_uri(entity_type: str) -> URIRef:
    """
    Return canonical type URI for entity type.

    All types are UPPERCASE with no "Entity" suffix.

    Args:
        entity_type: Entity type string (e.g., "person", "ORGANIZATION")

    Returns:
        Type URI (e.g., koi:PERSON)
    """
    return KOI[entity_type.upper()]


class FusekiGraphRegenerator:
    """Regenerate Fuseki graph from PostgreSQL entity_registry and koi_relationships."""

    def __init__(self, fuseki_endpoint: str):
        """
        Initialize regenerator.

        Args:
            fuseki_endpoint: Target Fuseki endpoint URL
        """
        self.db_config = {
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", 5433)),
            "database": os.getenv("POSTGRES_DB", "eliza"),
            "user": os.getenv("POSTGRES_USER", "postgres"),
            "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        }

        self.fuseki_endpoint = fuseki_endpoint
        self.fuseki_update_endpoint = f"{fuseki_endpoint}/update"
        self.fuseki_data_endpoint = f"{fuseki_endpoint}/data"
        self.fuseki_user = FUSEKI_USER
        self.fuseki_password = FUSEKI_PASSWORD

        self.graph = Graph()
        self.graph.bind("koi", KOI)
        self.graph.bind("prov", PROV)
        self.graph.bind("schema", SCHEMA)

        self.stats = {
            "entities_exported": 0,
            "relationships_exported": 0,
            "triples_created": 0,
            "fuseki_cleared": False,
            "fuseki_loaded": False,
        }

    def connect_db(self):
        """Connect to PostgreSQL."""
        return psycopg2.connect(**self.db_config)

    def export_entities_to_rdf(self) -> Graph:
        """
        Export entity_registry to RDF graph.

        Conformance:
        - Uses existing fuseki_uri from entity_registry (already HTTPS)
        - Type URIs are UPPERCASE without "Entity" suffix
        """
        print("\n" + "=" * 80)
        print("STEP 1: Export Entity Registry to RDF")
        print("=" * 80)

        conn = self.connect_db()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT
                id,
                fuseki_uri,
                entity_text,
                entity_type,
                occurrence_count,
                first_seen_at,
                last_seen_at
            FROM entity_registry
            ORDER BY id;
        """)

        entities = cursor.fetchall()
        conn.close()

        print(f"\n📊 Exporting {len(entities)} entities from PostgreSQL...")

        for entity in entities:
            # Use existing fuseki_uri (already HTTPS per entity_registry schema)
            if entity["fuseki_uri"]:
                entity_uri = URIRef(entity["fuseki_uri"])
            else:
                # Fallback: log warning and skip
                print(f"⚠️  Entity {entity['id']} has no fuseki_uri, skipping")
                continue

            # Add triples
            # Label
            self.graph.add((entity_uri, RDFS.label, Literal(entity["entity_text"])))

            # Type - UPPERCASE, no "Entity" suffix
            entity_type = entity["entity_type"]
            type_uri = get_type_uri(entity_type)
            self.graph.add((entity_uri, RDF.type, type_uri))

            # Occurrence count
            self.graph.add((entity_uri, KOI.occurrenceCount, Literal(entity["occurrence_count"])))

            # Timestamps
            if entity["first_seen_at"]:
                self.graph.add((entity_uri, KOI.firstSeen, Literal(entity["first_seen_at"])))
            if entity["last_seen_at"]:
                self.graph.add((entity_uri, KOI.lastSeen, Literal(entity["last_seen_at"])))

            self.stats["entities_exported"] += 1

        print(f"✓ Exported {self.stats['entities_exported']} entities")

        return self.graph

    def export_relationships_to_rdf(self) -> Graph:
        """
        Export koi_relationships to RDF graph.

        Conformance:
        - Predicates use koi# namespace (lowercase snake_case)
        - Subject/object URIs from entity_registry (already HTTPS)
        """
        print("\n" + "=" * 80)
        print("STEP 1.5: Export Relationships to RDF")
        print("=" * 80)

        conn = self.connect_db()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        cursor.execute("""
            SELECT
                s.fuseki_uri AS subject_uri,
                r.predicate,
                o.fuseki_uri AS object_uri,
                r.confidence
            FROM koi_relationships r
            JOIN entity_registry s ON r.subject_entity_id = s.id
            JOIN entity_registry o ON r.object_entity_id = o.id
        """)

        relationships = cursor.fetchall()
        conn.close()

        print(f"\n📊 Exporting {len(relationships)} relationships from koi_relationships...")

        for rel in relationships:
            subject_uri_str = rel['subject_uri']
            object_uri_str = rel['object_uri']
            predicate_name = rel['predicate']

            if not subject_uri_str or not object_uri_str:
                print(f"⚠️  Relationship has null URI, skipping")
                continue

            subject = URIRef(subject_uri_str)
            predicate = KOI[predicate_name]  # koi# namespace
            obj = URIRef(object_uri_str)

            self.graph.add((subject, predicate, obj))

            # Optionally add confidence as reification (skip for now to keep graph simple)
            self.stats["relationships_exported"] += 1

        print(f"✓ Exported {self.stats['relationships_exported']} relationships")

        return self.graph

    def save_rdf_to_file(self, output_path: Path):
        """Save RDF graph to Turtle file."""
        self.stats["triples_created"] = len(self.graph)
        print(f"\n💾 Saving RDF graph to: {output_path}")
        print(f"   Total triples: {self.stats['triples_created']:,}")

        self.graph.serialize(destination=str(output_path), format="turtle")

        file_size = output_path.stat().st_size / (1024 * 1024)  # MB
        print(f"✓ Saved {file_size:.2f} MB")

    def clear_fuseki_dataset(self):
        """Clear existing Fuseki dataset."""
        print("\n" + "=" * 80)
        print("STEP 2: Clear Existing Fuseki Dataset")
        print("=" * 80)
        print(f"⚠️  Clearing dataset at: {self.fuseki_endpoint}")

        sparql = SPARQLWrapper(self.fuseki_update_endpoint)
        sparql.setMethod(POST)
        sparql.setHTTPAuth("BASIC")
        sparql.setCredentials(self.fuseki_user, self.fuseki_password)

        # SPARQL UPDATE to delete all triples
        delete_query = "DELETE WHERE { ?s ?p ?o }"

        try:
            sparql.setQuery(delete_query)
            sparql.query()

            self.stats["fuseki_cleared"] = True
            print("✓ Fuseki dataset cleared")

        except Exception as e:
            print(f"✗ Failed to clear Fuseki: {e}")
            raise

    def load_rdf_to_fuseki(self, rdf_file: Path):
        """Load RDF file into Fuseki dataset."""
        print("\n" + "=" * 80)
        print("STEP 3: Load New RDF Graph to Fuseki")
        print("=" * 80)

        import requests

        try:
            # Read RDF file
            with open(rdf_file, "rb") as f:
                rdf_data = f.read()

            # POST to Fuseki data endpoint
            headers = {"Content-Type": "text/turtle"}
            response = requests.post(
                self.fuseki_data_endpoint,
                data=rdf_data,
                headers=headers,
                auth=(self.fuseki_user, self.fuseki_password)
            )

            if response.status_code in [200, 201, 204]:
                self.stats["fuseki_loaded"] = True
                print(f"✓ Loaded RDF graph to Fuseki (HTTP {response.status_code})")
            else:
                print(f"✗ Failed to load RDF: HTTP {response.status_code}")
                print(f"Response: {response.text}")
                raise Exception(f"Fuseki load failed: {response.status_code}")

        except Exception as e:
            print(f"✗ Failed to load Fuseki: {e}")
            raise

    def validate_fuseki_graph(self) -> int:
        """Validate Fuseki graph by counting triples."""
        print("\n" + "=" * 80)
        print("STEP 4: Validate Fuseki Graph")
        print("=" * 80)

        sparql = SPARQLWrapper(f"{self.fuseki_endpoint}/sparql")
        sparql.setHTTPAuth("BASIC")
        sparql.setCredentials(self.fuseki_user, self.fuseki_password)

        count_query = "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }"

        try:
            sparql.setQuery(count_query)
            sparql.setReturnFormat("json")
            results = sparql.query().convert()

            triple_count = int(results["results"]["bindings"][0]["count"]["value"])

            print(f"\n📊 Fuseki triple count: {triple_count:,}")
            print(f"📊 Expected (from export): {self.stats['triples_created']:,}")

            if triple_count == self.stats["triples_created"]:
                print("✓ Triple counts match!")
            else:
                diff = abs(triple_count - self.stats['triples_created'])
                print(f"⚠️  Triple count mismatch (diff: {diff})")

            return triple_count

        except Exception as e:
            print(f"✗ Failed to validate Fuseki: {e}")
            raise

    def run(self):
        """Main execution flow."""
        print("=" * 80)
        print("FUSEKI GRAPH REGENERATION (FIX-001 Compliant)")
        print("=" * 80)
        print(f"\n🎯 Target Fuseki endpoint: {self.fuseki_endpoint}")
        print(f"📍 Mode: {'STAGING' if STAGING else 'PRODUCTION'}")
        print(f"\nStarted: {datetime.now().isoformat()}")

        output_dir = Path(f"/opt/projects/koi-processor/exports/fuseki_regen_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        output_dir.mkdir(parents=True, exist_ok=True)

        rdf_file = output_dir / "koi_graph.ttl"

        try:
            # Step 1: Export entities to RDF
            self.export_entities_to_rdf()

            # Step 1.5: Export relationships to RDF
            self.export_relationships_to_rdf()

            # Save to file
            self.save_rdf_to_file(rdf_file)

            # Step 2: Clear Fuseki
            self.clear_fuseki_dataset()

            # Step 3: Load new graph
            self.load_rdf_to_fuseki(rdf_file)

            # Step 4: Validate
            triple_count = self.validate_fuseki_graph()

            # Summary
            print("\n" + "=" * 80)
            print("SUMMARY")
            print("=" * 80)
            print(f"\n✓ Entities exported: {self.stats['entities_exported']:,}")
            print(f"✓ Relationships exported: {self.stats['relationships_exported']:,}")
            print(f"✓ Triples created: {self.stats['triples_created']:,}")
            print(f"✓ Fuseki cleared: {self.stats['fuseki_cleared']}")
            print(f"✓ Fuseki loaded: {self.stats['fuseki_loaded']}")
            print(f"✓ Fuseki triple count: {triple_count:,}")

            print(f"\n✓ RDF export saved: {rdf_file}")
            print(f"\nCompleted: {datetime.now().isoformat()}")

            return True

        except Exception as e:
            print(f"\n✗ Regeneration failed: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """Main entry point with safety latch for production."""
    print("=" * 80)
    print("KOI FUSEKI GRAPH REGENERATOR")
    print("=" * 80)
    print(f"\n🎯 Target Fuseki endpoint: {ENDPOINT}")
    print(f"📍 Mode: {'STAGING' if STAGING else 'PRODUCTION'}")

    # Safety latch: require --confirm-prod for production target
    if not STAGING and '--confirm-prod' not in sys.argv:
        print("\n❌ ERROR: Production target requires --confirm-prod flag")
        print("   Usage: python regenerate_fuseki_graph.py --confirm-prod")
        print("\n   Or set KOI_STAGING=true for staging mode:")
        print("   export KOI_STAGING=true")
        print("   python regenerate_fuseki_graph.py")
        sys.exit(1)

    if not STAGING:
        print("\n⚠️  WARNING: Running against PRODUCTION endpoint!")
        print("   Press Ctrl+C within 5 seconds to abort...")
        import time
        try:
            time.sleep(5)
        except KeyboardInterrupt:
            print("\n\nAborted by user.")
            sys.exit(1)

    regenerator = FusekiGraphRegenerator(ENDPOINT)
    success = regenerator.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
