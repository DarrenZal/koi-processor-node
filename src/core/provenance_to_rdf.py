#!/usr/bin/env python3
"""
Provenance to RDF Module
Writes document provenance data to Apache Jena knowledge graph

Run as a script it is a demo: with --apply it INSERTs example provenance for
'regen.github:README_md_v1' into the Fuseki dataset FUSEKI_URL/FUSEKI_DATASET names
(default http://localhost:3030/koi) when it answers, then queries it back. With no
arguments or --dry-run it only prints what it would do.
"""

import os
import json
import httpx
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from urllib.parse import quote

logger = logging.getLogger(__name__)

# Configuration
FUSEKI_URL = os.getenv('FUSEKI_URL', 'http://localhost:3030')
FUSEKI_DATASET = os.getenv('FUSEKI_DATASET', 'koi')
FUSEKI_UPDATE_URL = f"{FUSEKI_URL}/{FUSEKI_DATASET}/update"
FUSEKI_QUERY_URL = f"{FUSEKI_URL}/{FUSEKI_DATASET}/sparql"
FUSEKI_USER = os.getenv("FUSEKI_USER")
FUSEKI_PASSWORD = os.getenv("FUSEKI_PASSWORD")

class ProvenanceToRDF:
    """Writes document provenance to RDF knowledge graph"""

    def __init__(self):
        self.fuseki_url = FUSEKI_UPDATE_URL
        self.fuseki_query_url = FUSEKI_QUERY_URL
        self._auth: Optional[httpx.BasicAuth] = None
        if FUSEKI_USER and FUSEKI_PASSWORD:
            self._auth = httpx.BasicAuth(FUSEKI_USER, FUSEKI_PASSWORD)
        self.prefixes = """
        PREFIX koi: <http://koi.regen.network/ontology#>
        PREFIX doc: <http://koi.regen.network/document/>
        PREFIX sensor: <http://koi.regen.network/sensor/>
        PREFIX infra: <http://koi.regen.network/infrastructure/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX dc: <http://purl.org/dc/elements/1.1/>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        """

    async def write_document_provenance(
        self,
        rid: str,
        sensor_id: str,
        event_type: str,
        timestamp: str,
        title: Optional[str] = None,
        content_hash: Optional[str] = None,
        processors: Optional[List[str]] = None,
        storage_locations: Optional[List[str]] = None,
        cat_receipt_id: Optional[str] = None
    ) -> bool:
        """
        Write document provenance to RDF graph

        Args:
            rid: Document RID
            sensor_id: ID of sensor that collected the document
            event_type: NEW, UPDATE, or FORGET
            timestamp: ISO timestamp
            title: Document title
            content_hash: SHA-256 hash of content
            processors: List of processors that handled the document
            storage_locations: List of storage systems
            cat_receipt_id: CAT receipt ID for transformation tracking

        Returns:
            bool: True if successful
        """
        try:
            # Create RDF triples for document provenance
            doc_uri = f"<doc:{quote(rid)}>"
            sensor_uri = f"<sensor:{sensor_id}>"

            # Build INSERT DATA query
            insert_query = f"""
            {self.prefixes}

            INSERT DATA {{
                {doc_uri} a koi:Document ;
                    koi:rid "{rid}" ;
                    koi:sensedBy {sensor_uri} ;
                    koi:eventType "{event_type}" ;
                    koi:timestamp "{timestamp}"^^xsd:dateTime .
            """

            # Add optional properties
            if title:
                insert_query += f'\n    {doc_uri} rdfs:label "{self._escape_literal(title)}" .'

            if content_hash:
                insert_query += f'\n    {doc_uri} koi:contentHash "{content_hash}" .'

            if cat_receipt_id:
                insert_query += f'\n    {doc_uri} prov:wasGeneratedBy <cat:{cat_receipt_id}> .'

            # Add processor relationships
            if processors:
                for processor in processors:
                    proc_uri = f"<infra:{processor}>"
                    insert_query += f'\n    {doc_uri} koi:processedBy {proc_uri} .'

            # Add storage relationships
            if storage_locations:
                for storage in storage_locations:
                    storage_uri = f"<infra:{storage}>"
                    insert_query += f'\n    {doc_uri} koi:storedIn {storage_uri} .'

            insert_query += "\n}"

            # Execute SPARQL UPDATE
            async with httpx.AsyncClient(timeout=10.0, auth=self._auth) as client:
                response = await client.post(
                    self.fuseki_url,
                    data=insert_query,
                    headers={"Content-Type": "application/sparql-update"}
                )

                if response.status_code in [200, 204]:
                    logger.info(f"Successfully wrote provenance for {rid} to RDF graph")
                    return True
                else:
                    logger.error(f"Failed to write provenance: {response.status_code} - {response.text}")
                    return False

        except Exception as e:
            logger.error(f"Error writing provenance to RDF: {e}")
            return False

    async def write_cat_receipt(
        self,
        cat_id: str,
        input_rid: str,
        output_rid: str,
        transformation_type: str,
        timestamp: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Write CAT receipt to RDF graph

        Args:
            cat_id: CAT receipt ID
            input_rid: Input document RID
            output_rid: Output document RID
            transformation_type: Type of transformation
            timestamp: ISO timestamp
            metadata: Additional metadata

        Returns:
            bool: True if successful
        """
        try:
            cat_uri = f"<cat:{cat_id}>"
            input_uri = f"<doc:{quote(input_rid)}>"
            output_uri = f"<doc:{quote(output_rid)}>"

            insert_query = f"""
            {self.prefixes}

            INSERT DATA {{
                {cat_uri} a prov:Activity ;
                    rdfs:label "CAT Transformation: {transformation_type}" ;
                    prov:used {input_uri} ;
                    prov:generated {output_uri} ;
                    prov:startedAtTime "{timestamp}"^^xsd:dateTime ;
                    koi:transformationType "{transformation_type}" .

                {output_uri} prov:wasDerivedFrom {input_uri} ;
                    prov:wasGeneratedBy {cat_uri} .
            }}
            """

            async with httpx.AsyncClient(timeout=10.0, auth=self._auth) as client:
                response = await client.post(
                    self.fuseki_url,
                    data=insert_query,
                    headers={"Content-Type": "application/sparql-update"}
                )

                if response.status_code in [200, 204]:
                    logger.info(f"Successfully wrote CAT receipt {cat_id} to RDF graph")
                    return True
                else:
                    logger.error(f"Failed to write CAT receipt: {response.status_code}")
                    return False

        except Exception as e:
            logger.error(f"Error writing CAT receipt to RDF: {e}")
            return False

    async def query_document_provenance(self, rid: str) -> Optional[Dict[str, Any]]:
        """
        Query provenance for a specific document

        Args:
            rid: Document RID

        Returns:
            Dict with provenance information or None
        """
        try:
            query = f"""
            {self.prefixes}

            SELECT ?sensor ?processor ?storage ?timestamp ?eventType ?catReceipt
            WHERE {{
                <doc:{quote(rid)}> koi:rid "{rid}" .
                OPTIONAL {{ <doc:{quote(rid)}> koi:sensedBy ?sensor }}
                OPTIONAL {{ <doc:{quote(rid)}> koi:processedBy ?processor }}
                OPTIONAL {{ <doc:{quote(rid)}> koi:storedIn ?storage }}
                OPTIONAL {{ <doc:{quote(rid)}> koi:timestamp ?timestamp }}
                OPTIONAL {{ <doc:{quote(rid)}> koi:eventType ?eventType }}
                OPTIONAL {{ <doc:{quote(rid)}> prov:wasGeneratedBy ?catReceipt }}
            }}
            """

            async with httpx.AsyncClient(timeout=10.0, auth=self._auth) as client:
                response = await client.post(
                    self.fuseki_query_url,
                    data={"query": query},
                    headers={"Accept": "application/sparql-results+json"}
                )

                if response.status_code == 200:
                    results = response.json()
                    if results["results"]["bindings"]:
                        binding = results["results"]["bindings"][0]
                        return {
                            "rid": rid,
                            "sensor": binding.get("sensor", {}).get("value"),
                            "processors": [b.get("processor", {}).get("value")
                                         for b in results["results"]["bindings"]
                                         if "processor" in b],
                            "storage": [b.get("storage", {}).get("value")
                                      for b in results["results"]["bindings"]
                                      if "storage" in b],
                            "timestamp": binding.get("timestamp", {}).get("value"),
                            "eventType": binding.get("eventType", {}).get("value"),
                            "catReceipt": binding.get("catReceipt", {}).get("value")
                        }
                    return None
                else:
                    logger.error(f"Query failed: {response.status_code}")
                    return None

        except Exception as e:
            logger.error(f"Error querying provenance: {e}")
            return None

    def _escape_literal(self, text: str) -> str:
        """Escape text for RDF literals"""
        if text:
            return text.replace('"', '\\"').replace('\n', '\\n').replace('\r', '\\r')
        return ""

    async def check_fuseki_connection(self) -> bool:
        """Check if Fuseki is accessible"""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{FUSEKI_URL}/$/ping")
                return response.status_code == 200
        except:
            return False

# Example usage
async def test_provenance():
    """Test provenance writing"""
    prdf = ProvenanceToRDF()

    # Check Fuseki connection
    if not await prdf.check_fuseki_connection():
        print("Warning: Apache Jena Fuseki is not accessible")
        return

    # Write document provenance
    await prdf.write_document_provenance(
        rid="regen.github:README_md_v1",
        sensor_id="github-sensor",
        event_type="NEW",
        timestamp=datetime.now(timezone.utc).isoformat(),
        title="README.md",
        content_hash="abc123...",
        processors=["event-bridge", "bge-embeddings"],
        storage_locations=["postgresql", "apache-jena"],
        cat_receipt_id="cat_2025_09_26_example"
    )

    # Query provenance
    provenance = await prdf.query_document_provenance("regen.github:README_md_v1")
    print(f"Provenance: {json.dumps(provenance, indent=2)}")

def cli_main(argv=None) -> int:
    """Command-line entry: a dry run unless --apply is given."""
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(
        description="Provenance-to-RDF demo: insert example provenance for "
                    "'regen.github:README_md_v1' into Fuseki. Default is a dry run.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would happen and exit (the default; wins over --apply)")
    parser.add_argument("--apply", action="store_true",
                        help="Ping Fuseki and, if it answers, insert and query the example")
    args = parser.parse_args(argv)

    auth = f"user {FUSEKI_USER}, password ***" if FUSEKI_USER and FUSEKI_PASSWORD else "no auth"
    print(f"Target: {FUSEKI_UPDATE_URL} ({auth})")
    print("Steps: GET /$/ping; if it answers, INSERT DATA example provenance for "
          "'regen.github:README_md_v1' (not removed afterwards) and query it back")
    if args.dry_run or not args.apply:
        print("\nDry run: nothing was sent. Re-run with --apply to do this.")
        return 0
    asyncio.run(test_provenance())
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(cli_main())
