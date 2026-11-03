#!/usr/bin/env python3
"""
KOI Knowledge MCP Server - stdio Transport
Implements MCP 2024-11-05 specification for ElizaOS integration

Provides tools for knowledge retrieval via Hybrid RAG API:
- search_knowledge: Search using RRF + BGE + BM25
- get_memory: Retrieve specific document by RID
- get_stats: Get knowledge base statistics
"""

import sys
import json
import asyncio
import httpx
import logging
from typing import Dict, Any, List

# Configure logging to stderr (stdout is for JSON-RPC only)
logging.basicConfig(
    level=logging.DEBUG,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stderr)]
)
logger = logging.getLogger(__name__)

# Configuration
HYBRID_RAG_API_URL = "http://localhost:8301/api/koi/query"
STATS_API_URL = "http://localhost:8301/api/koi/stats"

class MCPServer:
    """stdio MCP Server for KOI Knowledge Graph"""

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
                    "name": "koi-knowledge",
                    "version": "3.0.0"
                }
            }
        }

    async def handle_tools_list(self, request_id: Any) -> Dict[str, Any]:
        """Handle tools/list request"""
        logger.info("Handling tools/list request")

        tools = [
            {
                "name": "search_knowledge",
                "description": "Search the KOI knowledge graph using hybrid RAG (RRF + BGE embeddings + keyword search). Returns relevant documents with confidence scores and RIDs.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The search query text"
                        },
                        "limit": {
                            "type": "number",
                            "description": "Maximum number of results to return (default: 5)",
                            "default": 5
                        }
                    },
                    "required": ["query"]
                }
            },
            {
                "name": "get_memory",
                "description": "Retrieve a specific memory/document by its RID (Resource Identifier). Returns the full document content.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "rid": {
                            "type": "string",
                            "description": "The RID of the document (e.g., 'orn:web.page:registry.regen.network/abc123#chunk0')"
                        }
                    },
                    "required": ["rid"]
                }
            },
            {
                "name": "get_stats",
                "description": "Get statistics about the KOI knowledge base including total memories, sensors, embedding coverage, and recent activity.",
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

    async def search_knowledge(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Search knowledge using Hybrid RAG API"""
        try:
            logger.info(f"Searching knowledge: query='{query}', limit={limit}")

            response = await self.http_client.post(
                HYBRID_RAG_API_URL,
                json={
                    "question": query,
                    "limit": limit
                }
            )
            response.raise_for_status()
            data = response.json()

            results = data.get("results", [])
            confidence = data.get("average_confidence", 0.0)

            logger.info(f"Retrieved {len(results)} results with avg confidence {confidence:.3f}")

            # Debug: Log first result details
            if results:
                first_result = results[0]
                logger.info(f"DEBUG: First result RID: {first_result.get('rid', 'N/A')}")
                logger.info(f"DEBUG: First result score: {first_result.get('score', 0.0):.3f}")
                logger.info(f"DEBUG: First result content preview: {first_result.get('content', '')[:100]}...")

            return results

        except Exception as e:
            logger.error(f"Error searching knowledge: {e}")
            raise

    async def get_memory_by_rid(self, rid: str) -> Dict[str, Any]:
        """Get specific memory by RID"""
        try:
            logger.info(f"Retrieving memory by RID: {rid}")

            # Search for the specific RID
            response = await self.http_client.post(
                HYBRID_RAG_API_URL,
                json={
                    "question": rid,
                    "limit": 1
                }
            )
            response.raise_for_status()
            data = response.json()

            results = data.get("results", [])
            if results:
                return results[0]
            else:
                return {"error": f"No memory found with RID: {rid}"}

        except Exception as e:
            logger.error(f"Error retrieving memory: {e}")
            raise

    async def get_stats(self) -> Dict[str, Any]:
        """Get knowledge base statistics"""
        try:
            logger.info("Retrieving knowledge base statistics")

            response = await self.http_client.get(STATS_API_URL)
            response.raise_for_status()
            stats = response.json()

            logger.info(f"Retrieved stats: {stats.get('total_memories', 0)} total memories")

            return stats

        except Exception as e:
            logger.error(f"Error retrieving stats: {e}")
            raise

    def format_search_results(self, results: List[Dict[str, Any]], query: str) -> str:
        """Format search results for MCP response"""
        if not results:
            return f"No results found for query: {query}"

        formatted = f"# Knowledge Search Results for: {query}\n\n"

        for i, result in enumerate(results, 1):
            rid = result.get("rid", "unknown")
            content = result.get("content", "")
            score = result.get("score", 0.0)
            metadata = result.get("metadata", {})

            formatted += f"## Result {i} (confidence: {score:.3f})\n"
            formatted += f"**RID**: {rid}\n"
            # Increased from 500 to 2000 chars to provide more context for LLM
            formatted += f"**Content**: {content[:2000]}{'...' if len(content) > 2000 else ''}\n"

            if metadata:
                formatted += f"**Metadata**: {json.dumps(metadata, indent=2)}\n"

            formatted += "\n---\n\n"

        # Debug: Log formatted output size
        logger.info(f"DEBUG: Formatted {len(results)} results into {len(formatted)} characters")
        logger.info(f"DEBUG: Formatted output preview (first 300 chars): {formatted[:300]}...")

        return formatted

    async def handle_tools_call(self, request_id: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handle tools/call request"""
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        logger.info(f"Calling tool: {tool_name} with arguments: {arguments}")

        try:
            if tool_name == "search_knowledge":
                query = arguments.get("query", "")
                limit = arguments.get("limit", 5)

                logger.info(f"DEBUG: search_knowledge called with limit={limit}")

                results = await self.search_knowledge(query, limit)
                formatted_results = self.format_search_results(results, query)

                logger.info(f"DEBUG: Returning MCP response with {len(formatted_results)} chars to agent")

                # Write formatted results to debug file
                import os
                import time
                debug_dir = "/tmp/mcp_debug"
                os.makedirs(debug_dir, exist_ok=True)
                timestamp = time.strftime("%Y%m%d-%H%M%S")
                with open(f"{debug_dir}/last_search.txt", "w") as f:
                    f.write(f"Query: {query}\nLimit: {limit}\n\n{formatted_results}")
                with open(f"{debug_dir}/search_{timestamp}.txt", "w") as f:
                    f.write(f"=== MCP SEARCH DEBUG {timestamp} ===\n")
                    f.write(f"Query: {query}\n")
                    f.write(f"Limit: {limit}\n")
                    f.write(f"Results count: {len(results)}\n")
                    f.write(f"Formatted length: {len(formatted_results)} chars\n")
                    f.write(f"\n=== FORMATTED OUTPUT (this goes to agent) ===\n")
                    f.write(formatted_results)
                    f.write(f"\n\n=== END DEBUG ===\n")

                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": formatted_results
                            }
                        ]
                    }
                }

            elif tool_name == "get_memory":
                rid = arguments.get("rid", "")
                memory = await self.get_memory_by_rid(rid)

                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(memory, indent=2)
                            }
                        ]
                    }
                }

            elif tool_name == "get_stats":
                stats = await self.get_stats()

                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(stats, indent=2)
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
            logger.error(f"Error executing tool {tool_name}: {e}")
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32603,
                    "message": f"Internal error: {str(e)}"
                }
            }

    async def handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle a single JSON-RPC request"""
        method = request.get("method")
        request_id = request.get("id")
        params = request.get("params", {})

        if method == "initialize":
            return await self.handle_initialize(request_id, params)
        elif method == "tools/list":
            return await self.handle_tools_list(request_id)
        elif method == "tools/call":
            return await self.handle_tools_call(request_id, params)
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
        """Main stdio loop"""
        logger.info("KOI Knowledge MCP Server starting (stdio mode)")
        logger.info(f"Hybrid RAG API: {HYBRID_RAG_API_URL}")

        try:
            while True:
                # Read one line from stdin
                line = sys.stdin.readline()

                if not line:
                    # EOF - client disconnected
                    logger.info("Client disconnected (EOF)")
                    break

                line = line.strip()
                if not line:
                    continue

                try:
                    # Parse JSON-RPC request
                    request = json.loads(line)

                    # Handle request
                    response = await self.handle_request(request)

                    # Write response to stdout (one line)
                    print(json.dumps(response), flush=True)

                except json.JSONDecodeError as e:
                    logger.error(f"Invalid JSON: {e}")
                    error_response = {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {
                            "code": -32700,
                            "message": "Parse error"
                        }
                    }
                    print(json.dumps(error_response), flush=True)

                except Exception as e:
                    logger.error(f"Error handling request: {e}")
                    error_response = {
                        "jsonrpc": "2.0",
                        "id": None,
                        "error": {
                            "code": -32603,
                            "message": f"Internal error: {str(e)}"
                        }
                    }
                    print(json.dumps(error_response), flush=True)

        finally:
            await self.http_client.aclose()
            logger.info("MCP Server shutdown complete")

async def main():
    server = MCPServer()
    await server.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
