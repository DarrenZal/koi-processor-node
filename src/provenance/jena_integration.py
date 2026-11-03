"""
Apache Jena Integration Module for KOI Provenance
Manages RDF graph storage and SPARQL queries for provenance tracking
"""

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import quote
import httpx
from rdflib import Graph, Namespace, URIRef, Literal, RDF, RDFS, BNode
from rdflib.namespace import PROV, XSD, DC

logger = logging.getLogger(__name__)

# Jena Fuseki Configuration
JENA_QUERY_ENDPOINT = "http://localhost:3030/koi/sparql"
JENA_UPDATE_ENDPOINT = "http://localhost:3030/koi/update"
JENA_DATA_ENDPOINT = "http://localhost:3030/koi/data"

# Namespaces
KOI = Namespace("https://regen.network/koi#")
REGEN = Namespace("https://regen.network/ontology#")
SCHEMA = Namespace("http://schema.org/")


class JenaProvenanceManager:
    """
    Manages provenance data in Apache Jena Fuseki
    """

    def __init__(self,
                 query_endpoint: str = JENA_QUERY_ENDPOINT,
                 update_endpoint: str = JENA_UPDATE_ENDPOINT,
                 data_endpoint: str = JENA_DATA_ENDPOINT):
        self.query_endpoint = query_endpoint
        self.update_endpoint = update_endpoint
        self.data_endpoint = data_endpoint
        self.graph = Graph()
        self._bind_namespaces()

    def _bind_namespaces(self):
        """Bind common namespaces"""
        self.graph.bind("koi", KOI)
        self.graph.bind("regen", REGEN)
        self.graph.bind("prov", PROV)
        self.graph.bind("schema", SCHEMA)
        self.graph.bind("dc", DC)
        self.graph.bind("xsd", XSD)

    def generate_artifact_uri(self, artifact_type: str, rid: str) -> URIRef:
        """Generate consistent URI for an artifact"""
        # Replace colons with slashes for valid URI
        clean_rid = rid.replace(":", "/")
        return URIRef(f"{KOI}{artifact_type}/{clean_rid}")

    def generate_activity_uri(self, activity_type: str, timestamp: str = None) -> URIRef:
        """Generate URI for a transformation activity"""
        if not timestamp:
            timestamp = datetime.now(timezone.utc).isoformat()
        activity_id = hashlib.sha256(f"{activity_type}:{timestamp}".encode()).hexdigest()[:16]
        return URIRef(f"{KOI}activity/{activity_type}/{activity_id}")

    def generate_receipt_uri(self, receipt_hash: str) -> URIRef:
        """Generate URI for a CAT receipt"""
        return URIRef(f"{KOI}receipt/{receipt_hash[:32]}")

    async def store_artifact(self,
                           rid: str,
                           artifact_type: str,
                           metadata: Dict[str, Any],
                           parent_rid: Optional[str] = None) -> str:
        """
        Store an artifact with its metadata in Jena

        Args:
            rid: Resource identifier
            artifact_type: Type of artifact (document, chunk, embedding, etc.)
            metadata: Artifact metadata
            parent_rid: Parent artifact RID if derived

        Returns:
            URI of stored artifact
        """
        artifact_uri = self.generate_artifact_uri(artifact_type, rid)

        # Create triples for artifact
        g = Graph()
        g.bind("koi", KOI)
        g.bind("prov", PROV)

        # Add type
        artifact_class = getattr(KOI, artifact_type.title().replace("_", ""))
        g.add((artifact_uri, RDF.type, artifact_class))
        g.add((artifact_uri, RDF.type, KOI.Artifact))

        # Add RID
        g.add((artifact_uri, KOI.rid, Literal(rid)))

        # Add metadata
        if metadata.get("content_hash"):
            g.add((artifact_uri, KOI.contentHash, Literal(metadata["content_hash"])))
        if metadata.get("content_type"):
            g.add((artifact_uri, KOI.contentType, Literal(metadata["content_type"])))
        if metadata.get("size_bytes"):
            g.add((artifact_uri, KOI.sizeBytes, Literal(metadata["size_bytes"], datatype=XSD.integer)))
        if metadata.get("source_sensor"):
            g.add((artifact_uri, KOI.sourceSensor, Literal(metadata["source_sensor"])))
        if metadata.get("postgres_id"):
            g.add((artifact_uri, KOI.postgresId, Literal(metadata["postgres_id"])))
        if metadata.get("confidence"):
            g.add((artifact_uri, KOI.confidence, Literal(metadata["confidence"], datatype=XSD.float)))

        # Add provenance if parent exists
        if parent_rid:
            parent_uri = self.generate_artifact_uri("artifact", parent_rid)
            g.add((artifact_uri, PROV.wasDerivedFrom, parent_uri))

        # Add creation timestamp
        g.add((artifact_uri, PROV.generatedAtTime,
               Literal(datetime.now(timezone.utc), datatype=XSD.dateTime)))

        # Send to Jena
        await self._upload_graph(g)

        return str(artifact_uri)

    async def store_transformation(self,
                                  transformation_type: str,
                                  input_rid: str,
                                  output_rid: str,
                                  processor: str,
                                  metadata: Dict[str, Any]) -> Tuple[str, str]:
        """
        Store a transformation activity with CAT receipt

        Args:
            transformation_type: Type of transformation
            input_rid: Input artifact RID
            output_rid: Output artifact RID
            processor: Name of processor/agent
            metadata: Transformation metadata

        Returns:
            Tuple of (activity_uri, receipt_uri)
        """
        # Generate URIs
        activity_uri = self.generate_activity_uri(transformation_type)
        input_uri = self.generate_artifact_uri("artifact", input_rid)
        output_uri = self.generate_artifact_uri("artifact", output_rid)

        # Create CAT receipt hash
        receipt_content = f"{transformation_type}:{input_rid}:{output_rid}:{processor}"
        receipt_hash = hashlib.sha256(receipt_content.encode()).hexdigest()
        receipt_uri = self.generate_receipt_uri(receipt_hash)

        # Build graph
        g = Graph()
        self._bind_namespaces_to_graph(g)

        # Activity triples
        g.add((activity_uri, RDF.type, KOI.TransformationActivity))
        g.add((activity_uri, RDF.type, PROV.Activity))
        g.add((activity_uri, KOI.transformationType, Literal(transformation_type)))
        g.add((activity_uri, PROV.used, input_uri))
        g.add((activity_uri, PROV.startedAtTime,
               Literal(datetime.now(timezone.utc), datatype=XSD.dateTime)))

        # Link output to activity
        g.add((output_uri, PROV.wasGeneratedBy, activity_uri))
        g.add((output_uri, PROV.wasDerivedFrom, input_uri))

        # Agent/Processor
        processor_uri = URIRef(f"{KOI}processor/{processor.replace(' ', '_')}")
        g.add((processor_uri, RDF.type, PROV.Agent))
        g.add((processor_uri, RDF.type, KOI.Processor))
        g.add((processor_uri, RDFS.label, Literal(processor)))
        g.add((activity_uri, PROV.wasAssociatedWith, processor_uri))

        # CAT Receipt
        g.add((receipt_uri, RDF.type, KOI.CATReceipt))
        g.add((receipt_uri, KOI.receiptHash, Literal(receipt_hash)))
        g.add((receipt_uri, KOI.inputRID, Literal(input_rid)))
        g.add((receipt_uri, KOI.outputRID, Literal(output_rid)))
        g.add((receipt_uri, KOI.transformationType, Literal(transformation_type)))
        g.add((receipt_uri, PROV.generatedAtTime,
               Literal(datetime.now(timezone.utc), datatype=XSD.dateTime)))
        g.add((activity_uri, KOI.hasReceipt, receipt_uri))

        # Add metadata
        if metadata.get("confidence"):
            g.add((activity_uri, KOI.confidence,
                   Literal(metadata["confidence"], datatype=XSD.float)))
        if metadata.get("model"):
            g.add((activity_uri, KOI.extractionModel, Literal(metadata["model"])))
        if metadata.get("chunks_created"):
            g.add((activity_uri, KOI.chunksCreated,
                   Literal(metadata["chunks_created"], datatype=XSD.integer)))

        # Upload to Jena
        await self._upload_graph(g)

        return str(activity_uri), str(receipt_uri)

    async def store_extraction(self,
                             source_rid: str,
                             extraction_type: str,
                             extracted_data: Dict[str, Any],
                             model: str,
                             confidence: float = 1.0) -> List[str]:
        """
        Store extracted entities or relations

        Args:
            source_rid: Source document RID
            extraction_type: 'entity', 'relation', or 'jsonld'
            extracted_data: Extracted information
            model: Model used for extraction
            confidence: Confidence score

        Returns:
            List of created entity URIs
        """
        g = Graph()
        self._bind_namespaces_to_graph(g)
        source_uri = self.generate_artifact_uri("document", source_rid)
        created_uris = []

        # Create extraction activity
        activity_uri = self.generate_activity_uri(f"{extraction_type}_extraction")
        g.add((activity_uri, RDF.type, KOI.EntityExtraction))
        g.add((activity_uri, PROV.used, source_uri))
        g.add((activity_uri, KOI.extractionModel, Literal(model)))
        g.add((activity_uri, KOI.confidence, Literal(confidence, datatype=XSD.float)))

        if extraction_type == "entity":
            for entity in extracted_data.get("entities", []):
                entity_uri = self._create_entity(g, entity, source_uri, activity_uri)
                created_uris.append(str(entity_uri))

        elif extraction_type == "relation":
            for relation in extracted_data.get("relations", []):
                rel_uri = self._create_relation(g, relation, source_uri, activity_uri)
                created_uris.append(str(rel_uri))

        elif extraction_type == "jsonld":
            # Store JSON-LD directly
            jsonld_uri = URIRef(f"{KOI}jsonld/{hashlib.sha256(json.dumps(extracted_data).encode()).hexdigest()[:16]}")
            g.add((jsonld_uri, RDF.type, KOI.JSONLDDocument))
            g.add((jsonld_uri, PROV.wasGeneratedBy, activity_uri))
            g.add((jsonld_uri, KOI.extractedFrom, source_uri))
            created_uris.append(str(jsonld_uri))

        await self._upload_graph(g)
        return created_uris

    def _create_entity(self, g: Graph, entity: Dict, source_uri: URIRef, activity_uri: URIRef) -> URIRef:
        """Create entity in graph"""
        entity_id = entity.get("name", "").replace(" ", "_").lower()
        entity_uri = URIRef(f"{KOI}entity/{entity.get('type', 'unknown')}/{entity_id}")

        # Add entity triples
        g.add((entity_uri, RDF.type, KOI.ExtractedEntity))
        if entity.get("type"):
            entity_class = URIRef(f"{REGEN}{entity['type']}")
            g.add((entity_uri, RDF.type, entity_class))

        g.add((entity_uri, RDFS.label, Literal(entity.get("name", ""))))
        g.add((entity_uri, KOI.extractedFrom, source_uri))
        g.add((entity_uri, PROV.wasGeneratedBy, activity_uri))

        # Add properties
        for prop, value in entity.get("properties", {}).items():
            g.add((entity_uri, URIRef(f"{KOI}{prop}"), Literal(value)))

        return entity_uri

    def _create_relation(self, g: Graph, relation: Dict, source_uri: URIRef, activity_uri: URIRef) -> URIRef:
        """Create relation in graph"""
        rel_hash = hashlib.sha256(
            f"{relation.get('subject')}:{relation.get('predicate')}:{relation.get('object')}".encode()
        ).hexdigest()[:16]
        rel_uri = URIRef(f"{KOI}relation/{rel_hash}")

        g.add((rel_uri, RDF.type, KOI.ExtractedRelation))
        g.add((rel_uri, KOI.extractedFrom, source_uri))
        g.add((rel_uri, PROV.wasGeneratedBy, activity_uri))

        # Add relation details
        if relation.get("subject"):
            g.add((rel_uri, KOI.subject, Literal(relation["subject"])))
        if relation.get("predicate"):
            g.add((rel_uri, KOI.predicate, Literal(relation["predicate"])))
        if relation.get("object"):
            g.add((rel_uri, KOI.object, Literal(relation["object"])))

        return rel_uri

    def _bind_namespaces_to_graph(self, g: Graph):
        """Bind namespaces to a specific graph"""
        g.bind("koi", KOI)
        g.bind("regen", REGEN)
        g.bind("prov", PROV)
        g.bind("schema", SCHEMA)
        g.bind("dc", DC)
        g.bind("xsd", XSD)

    async def _upload_graph(self, g: Graph):
        """Upload graph to Jena Fuseki"""
        # Serialize graph to Turtle
        ttl_data = g.serialize(format="turtle")

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.data_endpoint,
                content=ttl_data,
                headers={"Content-Type": "text/turtle"},
                params={"graph": "default"},  # Or use named graphs
                auth=("admin", "admin")  # Default Jena credentials
            )

            if response.status_code not in [200, 201, 204]:
                logger.error(f"Failed to upload to Jena: {response.status_code} - {response.text}")
                raise Exception(f"Jena upload failed: {response.status_code}")

            logger.info(f"Successfully uploaded {len(g)} triples to Jena")

    async def query_provenance(self, rid: str) -> Dict[str, Any]:
        """
        Query complete provenance for a RID

        Args:
            rid: Resource identifier

        Returns:
            Provenance information including lineage and transformations
        """
        artifact_uri = self.generate_artifact_uri("artifact", rid)

        # SPARQL query for provenance
        query = f"""
        PREFIX koi: <https://regen.network/koi#>
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        SELECT ?artifact ?derivedFrom ?activity ?agent ?time ?type ?receipt
        WHERE {{
            <{artifact_uri}> prov:wasDerivedFrom* ?artifact .
            OPTIONAL {{ ?artifact prov:wasDerivedFrom ?derivedFrom }}
            OPTIONAL {{ ?artifact prov:wasGeneratedBy ?activity }}
            OPTIONAL {{ ?activity prov:wasAssociatedWith ?agent }}
            OPTIONAL {{ ?activity prov:startedAtTime ?time }}
            OPTIONAL {{ ?activity koi:transformationType ?type }}
            OPTIONAL {{ ?activity koi:hasReceipt ?receipt }}
        }}
        ORDER BY DESC(?time)
        """

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.query_endpoint,
                data={"query": query},
                headers={"Accept": "application/sparql-results+json"}
            )

            if response.status_code == 200:
                results = response.json()
                return self._parse_sparql_results(results)
            else:
                logger.error(f"SPARQL query failed: {response.status_code}")
                return {}

    def _parse_sparql_results(self, results: Dict) -> Dict[str, Any]:
        """Parse SPARQL JSON results into structured format"""
        provenance = {
            "artifacts": [],
            "transformations": [],
            "lineage": []
        }

        for binding in results.get("results", {}).get("bindings", []):
            if binding.get("artifact"):
                artifact = {
                    "uri": binding["artifact"]["value"],
                    "derived_from": binding.get("derivedFrom", {}).get("value"),
                    "activity": binding.get("activity", {}).get("value"),
                    "agent": binding.get("agent", {}).get("value"),
                    "time": binding.get("time", {}).get("value"),
                    "type": binding.get("type", {}).get("value"),
                    "receipt": binding.get("receipt", {}).get("value")
                }
                provenance["artifacts"].append(artifact)

        return provenance

    async def get_cat_receipts(self, rid: str) -> List[Dict[str, Any]]:
        """
        Get all CAT receipts for transformations involving a RID

        Args:
            rid: Resource identifier

        Returns:
            List of CAT receipts
        """
        query = f"""
        PREFIX koi: <https://regen.network/koi#>
        PREFIX prov: <http://www.w3.org/ns/prov#>

        SELECT ?receipt ?hash ?input ?output ?type ?time
        WHERE {{
            ?receipt a koi:CATReceipt ;
                    koi:receiptHash ?hash ;
                    koi:transformationType ?type ;
                    prov:generatedAtTime ?time .

            OPTIONAL {{ ?receipt koi:inputRID ?input }}
            OPTIONAL {{ ?receipt koi:outputRID ?output }}

            FILTER(?input = "{rid}" || ?output = "{rid}")
        }}
        ORDER BY DESC(?time)
        """

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.query_endpoint,
                data={"query": query},
                headers={"Accept": "application/sparql-results+json"}
            )

            if response.status_code == 200:
                results = response.json()
                receipts = []
                for binding in results.get("results", {}).get("bindings", []):
                    receipts.append({
                        "uri": binding.get("receipt", {}).get("value"),
                        "hash": binding.get("hash", {}).get("value"),
                        "input_rid": binding.get("input", {}).get("value"),
                        "output_rid": binding.get("output", {}).get("value"),
                        "type": binding.get("type", {}).get("value"),
                        "timestamp": binding.get("time", {}).get("value")
                    })
                return receipts
            else:
                return []