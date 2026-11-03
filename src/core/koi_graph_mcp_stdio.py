#!/usr/bin/env python3
"""
KOI Graph MCP Server - SPARQL Query Tool
Implements MCP 2024-11-05 specification for ElizaOS integration

Provides tools for Apache Jena knowledge graph queries:
- sparql_query: Execute SPARQL queries with natural language conversion
- graph_search: Search entities and relationships in the graph
- get_ontology: Retrieve the current ontology structure
"""

import sys
import json
import asyncio
import httpx
import logging
import os
from typing import Dict, Any, List, Optional
from urllib.parse import quote

# Configure logging to stderr (stdout is for JSON-RPC only)
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger(__name__)

# Configuration
JENA_QUERY_ENDPOINT = os.environ.get("JENA_ENDPOINT", "http://localhost:3030/koi/sparql")
GPT_API_KEY = os.environ.get("OPENAI_API_KEY", "")
GPT_MODEL = os.environ.get("GPT_MODEL", "gpt-4o-mini")

# Ontology prefixes and context
ONTOLOGY_CONTEXT = """
# Regen Network Knowledge Graph Ontology

## Namespaces
- regen: <https://regen.network/ontology/experimental#>
- prov: <http://www.w3.org/ns/prov#>
- schema: <http://schema.org/>
- rdfs: <http://www.w3.org/2000/01/rdf-schema#>

## Core Classes
- regen:Statement - A statement/claim extracted from documents
- schema:Organization - An organization entity
- schema:Project - A project entity
- schema:Person - A person entity
- prov:Entity - A provenance entity

## Core Properties
- regen:subject - Subject of a statement
- regen:predicate - Predicate/relation in a statement
- regen:object - Object of a statement
- regen:confidence - Confidence score (0.0-1.0)
- regen:entityType - Type of entity
- rdfs:label - Human-readable label
- prov:wasGeneratedBy - Provenance generation
- prov:hadPrimarySource - Primary source document
"""

EXAMPLE_QUERIES = """
## Example Natural Language to SPARQL:

1. "Find all organizations"
```sparql
PREFIX schema: <http://schema.org/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?org ?label WHERE {
  ?org a schema:Organization .
  OPTIONAL { ?org rdfs:label ?label }
} LIMIT 100
```

2. "Show statements about Regen Network"
```sparql
PREFIX regen: <https://regen.network/ontology/experimental#>
SELECT ?stmt ?predicate ?object WHERE {
  ?stmt regen:subject ?subject .
  ?stmt regen:predicate ?predicate .
  ?stmt regen:object ?object .
  FILTER(CONTAINS(LCASE(STR(?subject)), "regen network"))
} LIMIT 50
```

3. "Find high-confidence relationships"
```sparql
PREFIX regen: <https://regen.network/ontology/experimental#>
SELECT ?subject ?predicate ?object ?confidence WHERE {
  ?stmt regen:subject ?subject .
  ?stmt regen:predicate ?predicate .
  ?stmt regen:object ?object .
  ?stmt regen:confidence ?confidence .
  FILTER(?confidence > 0.8)
} ORDER BY DESC(?confidence) LIMIT 100
```
"""

class GraphMCPServer:
    """stdio MCP Server for SPARQL Graph Queries"""

    def __init__(self):
        self.http_client = httpx.AsyncClient(timeout=30.0)
        self.initialized = False

    async def handle_initialize(self, request_id: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle initialize request"""
        logger.info("Handling initialize request")
        self.initialized = True

        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": "koi-graph",
                    "version": "1.0.0"
                }
            }
        }

    async def handle_tools_list(self, request_id: Any) -> Dict[str, Any]:
        """Handle tools/list request"""
        logger.info("Handling tools/list request")

        tools = [
            {
                "name": "sparql_query",
                "description": "Execute SPARQL queries on the Apache Jena knowledge graph. Can convert natural language to SPARQL or execute raw SPARQL.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Natural language query or SPARQL query"
                        },
                        "is_sparql": {
                            "type": "boolean",
                            "description": "If true, query is already SPARQL. If false, convert from natural language.",
                            "default": False
                        },
                        "limit": {
                            "type": "number",
                            "description": "Maximum results (default: 50)",
                            "default": 50
                        }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "graph_search",
                "description": "Search for entities and relationships in the knowledge graph using keywords",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "keywords": {
                            "type": "string",
                            "description": "Keywords to search for in entities and relationships"
                        },
                        "entity_type": {
                            "type": "string",
                            "description": "Filter by entity type (Organization, Project, Person, Statement)",
                            "enum": ["Organization", "Project", "Person", "Statement", "Any"]
                        },
                        "min_confidence": {
                            "type": "number",
                            "description": "Minimum confidence score (0.0-1.0)",
                            "default": 0.5
                        },
                        "limit": {
                            "type": "number",
                            "description": "Maximum results (default: 25)",
                            "default": 25
                        }
                    },
                    "required": ["keywords"]
                }
            },
            {
                "name": "get_ontology",
                "description": "Get the current ontology structure including classes, properties, and statistics",
                "inputSchema": {
                    "type": "object",
                    "properties": {}
                }
            }
        ]

        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "result": {
                "tools": tools
            }
        }

    async def natural_language_to_sparql(self, nl_query: str, limit: int = 50) -> str:
        """Convert natural language query to SPARQL using GPT"""
        if not GPT_API_KEY:
            # Fallback to basic keyword search
            logger.warning("No OpenAI API key configured, using fallback keyword search")
            return self.create_keyword_sparql(nl_query, limit)

        prompt = f"""Convert this natural language query to a SPARQL query for the Regen Network knowledge graph.

{ONTOLOGY_CONTEXT}

{EXAMPLE_QUERIES}

Natural Language Query: "{nl_query}"

Important:
- Use appropriate prefixes
- Include LIMIT {limit} at the end
- Return ONLY the SPARQL query, no explanation

SPARQL Query:"""

        try:
            response = await self.http_client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {GPT_API_KEY}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": GPT_MODEL,
                    "messages": [
                        {"role": "system", "content": "You are a SPARQL query expert. Convert natural language to valid SPARQL queries."},
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.1,
                    "max_tokens": 500
                }
            )

            if response.status_code == 200:
                result = response.json()
                sparql = result["choices"][0]["message"]["content"].strip()
                # Clean up the SPARQL query
                if sparql.startswith("```sparql"):
                    sparql = sparql[9:]
                if sparql.startswith("```"):
                    sparql = sparql[3:]
                if sparql.endswith("```"):
                    sparql = sparql[:-3]
                return sparql.strip()
            else:
                logger.error(f"GPT API error: {response.status_code}")
                return self.create_keyword_sparql(nl_query, limit)

        except Exception as e:
            logger.error(f"Error converting to SPARQL: {e}")
            return self.create_keyword_sparql(nl_query, limit)

    def create_keyword_sparql(self, keywords: str, limit: int = 50) -> str:
        """Create a basic keyword search SPARQL query as fallback"""
        # Escape keywords for SPARQL
        keywords_lower = keywords.lower()

        return f"""
PREFIX regen: <https://regen.network/ontology/experimental#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>

SELECT DISTINCT ?subject ?predicate ?object ?label ?confidence WHERE {{
  {{
    ?stmt regen:subject ?subject .
    ?stmt regen:predicate ?predicate .
    ?stmt regen:object ?object .
    OPTIONAL {{ ?stmt regen:confidence ?confidence }}
    OPTIONAL {{ ?subject rdfs:label ?label }}
    FILTER(
      CONTAINS(LCASE(STR(?subject)), "{keywords_lower}") ||
      CONTAINS(LCASE(STR(?object)), "{keywords_lower}") ||
      CONTAINS(LCASE(STR(?label)), "{keywords_lower}")
    )
  }} UNION {{
    ?entity rdfs:label ?label .
    BIND(?entity as ?subject)
    BIND("has_label" as ?predicate)
    BIND(?label as ?object)
    FILTER(CONTAINS(LCASE(?label), "{keywords_lower}"))
  }}
}}
LIMIT {limit}
"""

    async def execute_sparql(self, sparql_query: str) -> Dict[str, Any]:
        """Execute SPARQL query against Apache Jena"""
        try:
            response = await self.http_client.post(
                JENA_QUERY_ENDPOINT,
                data={"query": sparql_query},
                headers={"Accept": "application/sparql-results+json"}
            )

            if response.status_code == 200:
                return response.json()
            else:
                return {
                    "error": f"SPARQL query failed: {response.status_code}",
                    "details": response.text
                }
        except Exception as e:
            logger.error(f"Error executing SPARQL: {e}")
            return {"error": str(e)}

    async def get_ontology_stats(self) -> Dict[str, Any]:
        """Get ontology statistics from the graph"""
        queries = {
            "total_triples": "SELECT (COUNT(*) as ?count) WHERE { ?s ?p ?o }",
            "total_entities": """
                PREFIX schema: <http://schema.org/>
                SELECT (COUNT(DISTINCT ?entity) as ?count) WHERE {
                    { ?entity a schema:Organization } UNION
                    { ?entity a schema:Project } UNION
                    { ?entity a schema:Person }
                }
            """,
            "total_statements": """
                PREFIX regen: <https://regen.network/ontology/experimental#>
                SELECT (COUNT(?stmt) as ?count) WHERE {
                    ?stmt a regen:Statement
                }
            """,
            "classes": """
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                SELECT ?class (COUNT(?instance) as ?count) WHERE {
                    ?instance rdf:type ?class
                }
                GROUP BY ?class
                ORDER BY DESC(?count)
                LIMIT 20
            """,
            "predicates": """
                SELECT ?predicate (COUNT(*) as ?count) WHERE {
                    ?s ?predicate ?o
                }
                GROUP BY ?predicate
                ORDER BY DESC(?count)
                LIMIT 20
            """
        }

        stats = {}
        for key, query in queries.items():
            result = await self.execute_sparql(query)
            if "results" in result and "bindings" in result["results"]:
                if key in ["total_triples", "total_entities", "total_statements"]:
                    bindings = result["results"]["bindings"]
                    if bindings:
                        stats[key] = int(bindings[0]["count"]["value"])
                else:
                    stats[key] = [
                        {
                            "name": binding.get("class", binding.get("predicate", {})).get("value", ""),
                            "count": int(binding["count"]["value"])
                        }
                        for binding in result["results"]["bindings"]
                    ]

        return stats

    async def handle_tools_call(self, request_id: Any, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Handle tool calls"""
        logger.info(f"Handling tool call: {tool_name} with args: {arguments}")

        try:
            if tool_name == "sparql_query":
                query = arguments.get("query", "")
                is_sparql = arguments.get("is_sparql", False)
                limit = arguments.get("limit", 50)

                # Convert to SPARQL if needed
                if not is_sparql:
                    sparql = await self.natural_language_to_sparql(query, limit)
                else:
                    sparql = query

                # Execute SPARQL
                result = await self.execute_sparql(sparql)

                # Format results
                if "results" in result and "bindings" in result["results"]:
                    bindings = result["results"]["bindings"]
                    formatted = f"## SPARQL Query Results ({len(bindings)} results)\n\n"

                    if bindings:
                        # Get column names from first result
                        columns = list(bindings[0].keys())
                        formatted += f"Columns: {', '.join(columns)}\n\n"

                        # Format each result
                        for i, binding in enumerate(bindings[:20], 1):  # Show first 20
                            formatted += f"### Result {i}\n"
                            for col in columns:
                                if col in binding:
                                    value = binding[col].get("value", "")
                                    # Truncate long values
                                    if len(value) > 200:
                                        value = value[:197] + "..."
                                    formatted += f"- **{col}**: {value}\n"
                            formatted += "\n"

                        if len(bindings) > 20:
                            formatted += f"\n... and {len(bindings) - 20} more results\n"
                    else:
                        formatted += "No results found.\n"

                    formatted += f"\n**SPARQL Query Used:**\n```sparql\n{sparql}\n```"

                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": formatted
                                }
                            ]
                        }
                    }
                else:
                    error = result.get("error", "Unknown error")
                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": f"## Error executing SPARQL\n\n{error}\n\nQuery:\n```sparql\n{sparql}\n```"
                                }
                            ]
                        }
                    }

            elif tool_name == "graph_search":
                keywords = arguments.get("keywords", "")
                entity_type = arguments.get("entity_type", "Any")
                min_confidence = arguments.get("min_confidence", 0.5)
                limit = arguments.get("limit", 25)

                # Build SPARQL query for search
                if entity_type != "Any":
                    type_filter = f"""
                    ?entity a schema:{entity_type} .
                    """
                else:
                    type_filter = ""

                sparql = f"""
PREFIX regen: <https://regen.network/ontology/experimental#>
PREFIX schema: <http://schema.org/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?entity ?type ?label ?confidence WHERE {{
    {type_filter}
    ?entity rdfs:label ?label .
    OPTIONAL {{ ?entity regen:entityType ?type }}
    OPTIONAL {{ ?entity regen:confidence ?confidence }}
    FILTER(CONTAINS(LCASE(?label), "{keywords.lower()}"))
    {"FILTER(?confidence >= " + str(min_confidence) + ")" if min_confidence > 0 else ""}
}}
ORDER BY DESC(?confidence)
LIMIT {limit}
"""

                result = await self.execute_sparql(sparql)

                if "results" in result and "bindings" in result["results"]:
                    bindings = result["results"]["bindings"]
                    formatted = f"## Graph Search Results for '{keywords}' ({len(bindings)} matches)\n\n"

                    for i, binding in enumerate(bindings, 1):
                        entity = binding.get("entity", {}).get("value", "").split("/")[-1][:50]
                        label = binding.get("label", {}).get("value", "")
                        entity_type = binding.get("type", {}).get("value", "Unknown")
                        confidence = binding.get("confidence", {}).get("value", "N/A")

                        formatted += f"{i}. **{label}**\n"
                        formatted += f"   - Type: {entity_type}\n"
                        formatted += f"   - ID: {entity}\n"
                        formatted += f"   - Confidence: {confidence}\n\n"

                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": formatted
                                }
                            ]
                        }
                    }
                else:
                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "result": {
                            "content": [
                                {
                                    "type": "text",
                                    "text": f"No results found for '{keywords}'"
                                }
                            ]
                        }
                    }

            elif tool_name == "get_ontology":
                stats = await self.get_ontology_stats()

                formatted = "## Knowledge Graph Ontology & Statistics\n\n"
                formatted += f"### Overview\n"
                formatted += f"- **Total Triples**: {stats.get('total_triples', 0):,}\n"
                formatted += f"- **Total Entities**: {stats.get('total_entities', 0):,}\n"
                formatted += f"- **Total Statements**: {stats.get('total_statements', 0):,}\n\n"

                formatted += "### Top Classes\n"
                for cls in stats.get("classes", [])[:10]:
                    formatted += f"- {cls['name'].split('/')[-1].split('#')[-1]}: {cls['count']:,}\n"

                formatted += "\n### Top Properties\n"
                for prop in stats.get("predicates", [])[:10]:
                    formatted += f"- {prop['name'].split('/')[-1].split('#')[-1]}: {prop['count']:,}\n"

                formatted += f"\n### Ontology Context\n```\n{ONTOLOGY_CONTEXT}\n```"

                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": formatted
                            }
                        ]
                    }
                }

            else:
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": {
                        "code": -32601,
                        "message": f"Unknown tool: {tool_name}"
                    }
                }

        except Exception as e:
            logger.error(f"Error in tool call: {e}", exc_info=True)
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32603,
                    "message": str(e)
                }
            }

    async def handle_request(self, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Handle incoming JSON-RPC request"""
        method = request.get("method")
        request_id = request.get("id")
        params = request.get("params", {})

        if method == "initialize":
            return await self.handle_initialize(request_id, params)
        elif method == "initialized":
            # No response needed for initialized notification
            logger.info("Received initialized notification")
            return None
        elif method == "tools/list":
            return await self.handle_tools_list(request_id)
        elif method == "tools/call":
            tool_name = params.get("name")
            arguments = params.get("arguments", {})
            return await self.handle_tools_call(request_id, tool_name, arguments)
        else:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32601,
                    "message": f"Method not found: {method}"
                }
            }

    async def run(self):
        """Main server loop - read from stdin, write to stdout"""
        logger.info("KOI Graph MCP Server starting...")

        while True:
            try:
                # Read line from stdin
                line = await asyncio.get_event_loop().run_in_executor(None, sys.stdin.readline)
                if not line:
                    logger.info("EOF received, shutting down")
                    break

                line = line.strip()
                if not line:
                    continue

                # Parse JSON-RPC request
                try:
                    request = json.loads(line)
                    logger.debug(f"Received: {request}")
                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON: {e}")
                    continue

                # Handle request
                response = await self.handle_request(request)

                # Send response if not a notification
                if response:
                    response_str = json.dumps(response)
                    print(response_str, flush=True)
                    logger.debug(f"Sent: {response_str}")

            except KeyboardInterrupt:
                logger.info("Received interrupt, shutting down")
                break
            except Exception as e:
                logger.error(f"Unexpected error: {e}", exc_info=True)

        await self.http_client.aclose()

def main():
    """Main entry point"""
    server = GraphMCPServer()
    asyncio.run(server.run())

if __name__ == "__main__":
    main()