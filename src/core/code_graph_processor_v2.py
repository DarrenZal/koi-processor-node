"""
Code Graph Processor v2 - With Full Provenance Tracking
Extracts code entities from GitHub events and generates CAT receipts for complete traceability
"""

import re
import json
import hashlib
import asyncpg
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timezone
from loguru import logger

# Import CAT receipt creation
from create_cat_receipt import create_cat_receipt


class CodeGraphProcessor:
    """
    Enhanced Code Graph Processor with CAT receipt generation

    Features:
    - Extracts code entities (Functions, Classes, Keepers, Messages, etc.)
    - Generates unique RIDs for each entity
    - Creates CAT receipts for provenance tracking
    - Stores provenance metadata with graph nodes/edges
    - Supports Python, Go, TypeScript/JavaScript
    """

    def __init__(self, db_config: Dict[str, Any], graph_name: str = 'regen_graph'):
        self.db_config = db_config
        self.graph_name = graph_name
        self.processor_version = "2.0.0"
        self.processor_name = "CodeGraphProcessor"

        # Supported file extensions and languages
        self.code_extensions = {
            '.py': 'python',
            '.go': 'go',
            '.ts': 'typescript',
            '.tsx': 'typescript',
            '.js': 'javascript',
            '.jsx': 'javascript',
            '.rs': 'rust',
        }

        # Repos enabled for graph indexing
        self.graph_enabled_repos = [
            'regen-ledger',
            'regen-web',
            'regen-data-standards',
            'koi-sensors',
            'koi-processor',
            'koi-research',
            'regen-koi-mcp'
        ]

    async def process_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process GitHub event and extract code entities with full provenance

        Args:
            event: KOI event containing:
                - rid: Resource ID
                - repo: Repository name
                - files: List of file objects
                - event_type: Event type (github_push, etc.)
                - source_node: Source sensor
                - timestamp: Event timestamp
                - metadata: Additional metadata (commit_sha, commit_date, branch, etc.)

        Returns:
            Dictionary with extraction results and CAT receipt IDs
        """
        # Handle KOI protocol format (with bundle) vs simplified format
        if "bundle" in event and event["bundle"]:
            # KOI protocol format - extract from bundle
            bundle = event["bundle"]
            manifest = bundle.get("manifest", {})
            contents = bundle.get("contents", {})

            # Extract file path from manifest metadata
            manifest_metadata = manifest.get("metadata", {})
            file_path = manifest_metadata.get("file_path", "")

            # Extract repo from file path (e.g., "regen-network/regen-ledger/x/ecocredit/msg.go")
            repo = self.extract_repo_from_path(file_path)

            # Convert to simplified format
            files = [{
                "path": file_path,
                "content": contents.get("raw", "")
            }] if file_path else []

            metadata = manifest_metadata
        else:
            # Simplified format - use as-is
            repo = event.get("repo")
            files = event.get("files", [])
            metadata = event.get("metadata", {})

        rid = event.get("rid")
        event_type = event.get("event_type")
        source_node = event.get("source_node")
        timestamp = event.get("timestamp")

        # Extract commit metadata
        commit_sha = metadata.get("commit_sha")
        commit_date = metadata.get("commit_date")
        branch = metadata.get("branch", "main")

        # Check if repo is enabled for graph indexing
        if repo not in self.graph_enabled_repos:
            logger.info(f"Repo {repo} not enabled for graph indexing - skipping")
            return {
                "success": True,
                "processed": False,
                "skipped": True,
                "reason": f"Repo {repo} not in enabled list",
                "entities_extracted": 0,
                "entities_loaded": 0,
                "cat_receipts_created": 0
            }

        logger.info(f"Processing event {rid} for repo {repo} with {len(files)} files")

        # Connect to database for CAT receipts and Apache AGE
        db_url = f"postgresql://{self.db_config['user']}:{self.db_config.get('password', '')}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
        conn = await asyncpg.connect(db_url)

        total_entities = 0
        total_edges = 0
        total_receipts = 0
        all_entities = []

        try:
            for file_info in files:
                file_path = file_info.get("path")
                content = file_info.get("content")

                if not content or not file_path:
                    continue

                # Check if file is a code file
                language = self._detect_language(file_path)
                if not language:
                    continue

                # Extract entities from file
                entities = self.extract_entities(content, file_path, repo, language)

                if not entities:
                    continue

                logger.info(f"Extracted {len(entities)} entities from {file_path}")

                # Generate file hash for verification
                file_hash = hashlib.sha256(content.encode()).hexdigest()

                # Create CAT receipts for EACH entity
                for entity in entities:
                    # Generate entity RID
                    entity_rid = self._generate_entity_rid(
                        repo,
                        file_path,
                        entity['line_number'],
                        entity['name']
                    )

                    # Generate entity content hash
                    entity_content = json.dumps({
                        'name': entity['name'],
                        'type': entity['type'],
                        'file_path': file_path,
                        'line_number': entity['line_number'],
                        'signature': entity.get('signature', ''),
                        'docstring': entity.get('docstring', '')
                    }, sort_keys=True)
                    entity_cid = hashlib.sha256(entity_content.encode()).hexdigest()

                    # Create CAT receipt for this entity
                    receipt_id = await create_cat_receipt(
                        conn=conn,
                        transformation_type="github_to_code_entity",
                        input_rid=rid,  # GitHub event RID
                        output_rid=entity_rid,
                        input_cid=commit_sha,
                        output_cid=entity_cid,
                        processor_name=self.processor_name,
                        processor_version=self.processor_version,
                        entities_extracted=1,
                        source_sensor=source_node or "github_sensor",
                        event_type=event_type,
                        metadata={
                            "file_path": file_path,
                            "line_number": entity['line_number'],
                            "entity_type": entity['type'],
                            "entity_name": entity['name'],
                            "language": language,
                            "extraction_method": entity.get('method', 'regex'),
                            "commit_sha": commit_sha,
                            "commit_date": commit_date,
                            "repo": repo,
                            "branch": branch,
                            "file_hash": file_hash,
                            "signature": entity.get('signature', ''),
                            "docstring": entity.get('docstring', ''),
                            "github_url": self._generate_github_url(
                                repo, commit_sha, file_path, entity['line_number']
                            )
                        }
                    )

                    # Add provenance to entity for Apache AGE storage
                    entity['source_rid'] = rid
                    entity['entity_rid'] = entity_rid
                    entity['cat_receipt_id'] = receipt_id
                    entity['commit_sha'] = commit_sha
                    entity['commit_date'] = commit_date
                    entity['branch'] = branch
                    entity['file_hash'] = file_hash
                    entity['extracted_at'] = datetime.now(timezone.utc).isoformat()
                    entity['processor_version'] = self.processor_version
                    entity['extraction_method'] = entity.get('method', 'regex')
                    entity['github_url'] = self._generate_github_url(
                        repo, commit_sha, file_path, entity['line_number']
                    )

                    total_entities += 1
                    total_receipts += 1
                    all_entities.append(entity)

            # Load entities to Apache AGE (with provenance)
            if all_entities:
                loaded_count = await self.load_entities_to_graph(conn, all_entities)
                logger.info(f"Loaded {loaded_count} entities to graph")

                # Extract and create relationships
                edges = self._extract_relationships(all_entities)

                for edge in edges:
                    # Generate edge RID
                    edge_rid = self._generate_edge_rid(
                        edge['from_rid'],
                        edge['to_rid'],
                        edge['type']
                    )

                    # Create CAT receipt for relationship
                    edge_receipt_id = await create_cat_receipt(
                        conn=conn,
                        transformation_type="code_entity_to_relationship",
                        input_rid=edge['from_rid'],
                        output_rid=edge_rid,
                        processor_name=self.processor_name,
                        processor_version=self.processor_version,
                        metadata={
                            "relationship_type": edge['type'],
                            "from_entity": edge['from'],
                            "to_entity": edge['to'],
                            "from_rid": edge['from_rid'],
                            "to_rid": edge['to_rid'],
                            "inferred": edge.get('inferred', False),
                            "confidence": edge.get('confidence', 1.0)
                        }
                    )

                    # Add provenance to edge
                    edge['edge_rid'] = edge_rid
                    edge['cat_receipt_id'] = edge_receipt_id
                    edge['extracted_at'] = datetime.now(timezone.utc).isoformat()

                    total_edges += 1
                    total_receipts += 1

                # Load edges to graph
                if edges:
                    edge_count = await self.load_edges_to_graph(conn, edges)
                    logger.info(f"Loaded {edge_count} edges to graph")

            return {
                "success": True,
                "processed": True,
                "rid": rid,
                "repo": repo,
                "entities_extracted": total_entities,
                "entities_loaded": total_entities,  # Same as extracted for this processor
                "relationships_created": total_edges,
                "cat_receipts_created": total_receipts,
                "files_processed": len([f for f in files if self._detect_language(f.get('path'))]),
                "commit_sha": commit_sha
            }

        except Exception as e:
            logger.error(f"Error processing event {rid}: {e}", exc_info=True)
            return {
                "success": False,
                "processed": False,
                "rid": rid,
                "error": str(e),
                "entities_extracted": total_entities,
                "entities_loaded": total_entities,
                "cat_receipts_created": total_receipts
            }
        finally:
            await conn.close()

    def _generate_entity_rid(
        self,
        repo: str,
        file_path: str,
        line_number: int,
        entity_name: str
    ) -> str:
        """Generate unique RID for code entity"""
        return f"rid://code/{repo}/{file_path}:{line_number}#{entity_name}"

    def _generate_edge_rid(
        self,
        from_rid: str,
        to_rid: str,
        rel_type: str
    ) -> str:
        """Generate unique RID for relationship"""
        content = f"{from_rid}-{rel_type}->{to_rid}"
        hash_id = hashlib.sha256(content.encode()).hexdigest()[:16]
        return f"rid://edge/{hash_id}"

    def _generate_github_url(
        self,
        repo: str,
        commit_sha: Optional[str],
        file_path: str,
        line_number: int
    ) -> str:
        """Generate GitHub URL to source code"""
        if not commit_sha:
            commit_sha = "main"
        return f"https://github.com/regen-network/{repo}/blob/{commit_sha}/{file_path}#L{line_number}"

    def _detect_language(self, file_path: str) -> Optional[str]:
        """Detect programming language from file extension"""
        for ext, lang in self.code_extensions.items():
            if file_path.endswith(ext):
                return lang
        return None

    def extract_entities(
        self,
        content: str,
        file_path: str,
        repo: str,
        language: str
    ) -> List[Dict[str, Any]]:
        """
        Extract code entities from file content

        Returns list of entities with:
        - name: Entity name
        - type: Entity type (Function, Class, Keeper, Message, etc.)
        - line_number: Line number in file
        - signature: Function/method signature
        - docstring: Documentation string
        - method: Extraction method used
        """
        if language == 'python':
            return self._extract_python_entities(content, file_path, repo)
        elif language == 'go':
            return self._extract_go_entities(content, file_path, repo)
        elif language in ['typescript', 'javascript']:
            return self._extract_ts_entities(content, file_path, repo)
        else:
            return []

    def _extract_python_entities(
        self,
        content: str,
        file_path: str,
        repo: str
    ) -> List[Dict[str, Any]]:
        """Extract entities from Python code"""
        entities = []
        lines = content.split('\n')

        # Patterns for Python entities
        class_pattern = re.compile(r'^\s*class\s+(\w+)(?:\(.*?\))?:')
        function_pattern = re.compile(r'^\s*def\s+(\w+)\s*\((.*?)\)(?:\s*->\s*[^:]+)?:')

        for i, line in enumerate(lines, 1):
            # Extract classes
            class_match = class_pattern.search(line)
            if class_match:
                class_name = class_match.group(1)
                entities.append({
                    'name': class_name,
                    'type': 'Class',
                    'line_number': i,
                    'file_path': file_path,
                    'repo': repo,
                    'signature': line.strip(),
                    'docstring': self._get_docstring(lines, i),
                    'method': 'regex'
                })

            # Extract functions
            func_match = function_pattern.search(line)
            if func_match:
                func_name = func_match.group(1)
                params = func_match.group(2)
                entities.append({
                    'name': func_name,
                    'type': 'Function',
                    'line_number': i,
                    'file_path': file_path,
                    'repo': repo,
                    'signature': line.strip(),
                    'params': params,
                    'docstring': self._get_docstring(lines, i),
                    'method': 'regex'
                })

        return entities

    def _extract_go_entities(
        self,
        content: str,
        file_path: str,
        repo: str
    ) -> List[Dict[str, Any]]:
        """Extract entities from Go code"""
        entities = []
        lines = content.split('\n')

        # Patterns for Go entities
        struct_pattern = re.compile(r'^\s*type\s+(\w+)\s+struct\s*{')
        interface_pattern = re.compile(r'^\s*type\s+(\w+)\s+interface\s*{')
        func_pattern = re.compile(r'^\s*func\s+(?:\(.*?\)\s*)?(\w+)\s*\(')
        keeper_pattern = re.compile(r'type\s+(\w*Keeper)\s+struct')
        msg_pattern = re.compile(r'type\s+(Msg\w+)\s+struct')

        for i, line in enumerate(lines, 1):
            # Extract Keepers (special case for Cosmos SDK)
            keeper_match = keeper_pattern.search(line)
            if keeper_match:
                keeper_name = keeper_match.group(1)
                entities.append({
                    'name': keeper_name,
                    'type': 'Keeper',
                    'line_number': i,
                    'file_path': file_path,
                    'repo': repo,
                    'signature': line.strip(),
                    'docstring': self._get_go_comment(lines, i),
                    'method': 'regex'
                })
                continue

            # Extract Messages (special case for Cosmos SDK)
            msg_match = msg_pattern.search(line)
            if msg_match:
                msg_name = msg_match.group(1)
                entities.append({
                    'name': msg_name,
                    'type': 'Message',
                    'line_number': i,
                    'file_path': file_path,
                    'repo': repo,
                    'signature': line.strip(),
                    'docstring': self._get_go_comment(lines, i),
                    'method': 'regex'
                })
                continue

            # Extract structs
            struct_match = struct_pattern.search(line)
            if struct_match:
                struct_name = struct_match.group(1)
                entities.append({
                    'name': struct_name,
                    'type': 'Struct',
                    'line_number': i,
                    'file_path': file_path,
                    'repo': repo,
                    'signature': line.strip(),
                    'docstring': self._get_go_comment(lines, i),
                    'method': 'regex'
                })

            # Extract interfaces
            interface_match = interface_pattern.search(line)
            if interface_match:
                interface_name = interface_match.group(1)
                entities.append({
                    'name': interface_name,
                    'type': 'Interface',
                    'line_number': i,
                    'file_path': file_path,
                    'repo': repo,
                    'signature': line.strip(),
                    'docstring': self._get_go_comment(lines, i),
                    'method': 'regex'
                })

            # Extract functions
            func_match = func_pattern.search(line)
            if func_match:
                func_name = func_match.group(1)
                entities.append({
                    'name': func_name,
                    'type': 'Function',
                    'line_number': i,
                    'file_path': file_path,
                    'repo': repo,
                    'signature': line.strip(),
                    'docstring': self._get_go_comment(lines, i),
                    'method': 'regex'
                })

        return entities

    def _extract_ts_entities(
        self,
        content: str,
        file_path: str,
        repo: str
    ) -> List[Dict[str, Any]]:
        """Extract entities from TypeScript/JavaScript code"""
        entities = []
        lines = content.split('\n')

        # Patterns for TS/JS entities
        class_pattern = re.compile(r'^\s*(?:export\s+)?class\s+(\w+)')
        interface_pattern = re.compile(r'^\s*(?:export\s+)?interface\s+(\w+)')
        function_pattern = re.compile(r'^\s*(?:export\s+)?(?:async\s+)?function\s+(\w+)\s*\(')
        const_func_pattern = re.compile(r'^\s*(?:export\s+)?const\s+(\w+)\s*=\s*(?:async\s*)?\(')

        for i, line in enumerate(lines, 1):
            # Extract classes
            class_match = class_pattern.search(line)
            if class_match:
                class_name = class_match.group(1)
                entities.append({
                    'name': class_name,
                    'type': 'Class',
                    'line_number': i,
                    'file_path': file_path,
                    'repo': repo,
                    'signature': line.strip(),
                    'docstring': self._get_jsdoc(lines, i),
                    'method': 'regex'
                })

            # Extract interfaces
            interface_match = interface_pattern.search(line)
            if interface_match:
                interface_name = interface_match.group(1)
                entities.append({
                    'name': interface_name,
                    'type': 'Interface',
                    'line_number': i,
                    'file_path': file_path,
                    'repo': repo,
                    'signature': line.strip(),
                    'docstring': self._get_jsdoc(lines, i),
                    'method': 'regex'
                })

            # Extract functions
            func_match = function_pattern.search(line)
            if func_match:
                func_name = func_match.group(1)
                entities.append({
                    'name': func_name,
                    'type': 'Function',
                    'line_number': i,
                    'file_path': file_path,
                    'repo': repo,
                    'signature': line.strip(),
                    'docstring': self._get_jsdoc(lines, i),
                    'method': 'regex'
                })

            # Extract const functions (arrow functions)
            const_func_match = const_func_pattern.search(line)
            if const_func_match:
                func_name = const_func_match.group(1)
                entities.append({
                    'name': func_name,
                    'type': 'Function',
                    'line_number': i,
                    'file_path': file_path,
                    'repo': repo,
                    'signature': line.strip(),
                    'docstring': self._get_jsdoc(lines, i),
                    'method': 'regex'
                })

        return entities

    def _get_docstring(self, lines: List[str], line_num: int) -> str:
        """Extract Python docstring"""
        if line_num >= len(lines):
            return ""

        # Look at next few lines for docstring
        for i in range(line_num, min(line_num + 5, len(lines))):
            line = lines[i].strip()
            if line.startswith('"""') or line.startswith("'''"):
                return line.strip('"\'')
        return ""

    def _get_go_comment(self, lines: List[str], line_num: int) -> str:
        """Extract Go comment above declaration"""
        if line_num <= 1:
            return ""

        comments = []
        i = line_num - 2  # 0-indexed, and look at line before
        while i >= 0:
            line = lines[i].strip()
            if line.startswith('//'):
                comments.insert(0, line[2:].strip())
                i -= 1
            else:
                break

        return ' '.join(comments)

    def _get_jsdoc(self, lines: List[str], line_num: int) -> str:
        """Extract JSDoc comment above declaration"""
        if line_num <= 1:
            return ""

        # Look for /** */ comment block
        i = line_num - 2
        if i >= 0 and lines[i].strip().endswith('*/'):
            comments = []
            while i >= 0:
                line = lines[i].strip()
                comments.insert(0, line)
                if line.startswith('/**'):
                    return ' '.join(comments)
                i -= 1

        return ""

    def _extract_relationships(self, entities: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Extract relationships between entities

        Returns list of edges with:
        - from: Source entity name
        - to: Target entity name
        - from_rid: Source entity RID
        - to_rid: Target entity RID
        - type: Relationship type (HANDLES, CALLS, IMPLEMENTS, etc.)
        - inferred: Whether relationship was inferred
        - confidence: Confidence score
        """
        edges = []

        # Build entity lookup
        entity_by_name = {e['name']: e for e in entities}

        # Extract Keeper -> Message HANDLES relationships
        keepers = [e for e in entities if e['type'] == 'Keeper']
        messages = [e for e in entities if e['type'] == 'Message']

        for keeper in keepers:
            for msg in messages:
                # Simple heuristic: if Keeper and Message are in same file, they might be related
                if keeper['file_path'] == msg['file_path']:
                    edges.append({
                        'from': keeper['name'],
                        'to': msg['name'],
                        'from_rid': keeper['entity_rid'],
                        'to_rid': msg['entity_rid'],
                        'type': 'HANDLES',
                        'inferred': True,
                        'confidence': 0.7
                    })

        # TODO: Add more sophisticated relationship extraction
        # - Function calls (requires AST parsing)
        # - Class inheritance
        # - Interface implementation
        # - Import/dependency tracking

        return edges

    async def load_entities_to_graph(
        self,
        conn: asyncpg.Connection,
        entities: List[Dict[str, Any]]
    ) -> int:
        """
        Load entities to Apache AGE graph with provenance metadata

        Returns count of entities loaded
        """
        if not entities:
            return 0

        loaded_count = 0

        for entity in entities:
            try:
                # Build Cypher CREATE query with all provenance properties
                cypher_query = f"""
                SELECT * FROM cypher('{self.graph_name}', $$
                    MERGE (e:Entity {{entity_rid: '{entity['entity_rid']}'}})
                    SET e.name = '{self._escape_cypher(entity['name'])}',
                        e.type = '{entity['type']}',
                        e.repo = '{entity['repo']}',
                        e.file_path = '{entity['file_path']}',
                        e.line_number = {entity['line_number']},
                        e.source_rid = '{entity['source_rid']}',
                        e.cat_receipt_id = '{entity['cat_receipt_id']}',
                        e.commit_sha = '{entity.get('commit_sha', '')}',
                        e.commit_date = '{entity.get('commit_date', '')}',
                        e.branch = '{entity.get('branch', 'main')}',
                        e.file_hash = '{entity.get('file_hash', '')}',
                        e.extracted_at = '{entity['extracted_at']}',
                        e.processor_version = '{entity['processor_version']}',
                        e.extraction_method = '{entity['extraction_method']}',
                        e.github_url = '{entity['github_url']}',
                        e.signature = '{self._escape_cypher(entity.get('signature', ''))}',
                        e.docstring = '{self._escape_cypher(entity.get('docstring', ''))}'
                    RETURN e
                $$) AS (e agtype);
                """

                await conn.execute(cypher_query)
                loaded_count += 1

            except Exception as e:
                logger.error(f"Error loading entity {entity['name']}: {e}")

        return loaded_count

    async def load_edges_to_graph(
        self,
        conn: asyncpg.Connection,
        edges: List[Dict[str, Any]]
    ) -> int:
        """
        Load relationship edges to Apache AGE graph with provenance

        Returns count of edges loaded
        """
        if not edges:
            return 0

        loaded_count = 0

        for edge in edges:
            try:
                cypher_query = f"""
                SELECT * FROM cypher('{self.graph_name}', $$
                    MATCH (from:Entity {{entity_rid: '{edge['from_rid']}'}})
                    MATCH (to:Entity {{entity_rid: '{edge['to_rid']}'}})
                    MERGE (from)-[r:{edge['type']} {{
                        edge_rid: '{edge['edge_rid']}',
                        cat_receipt_id: '{edge['cat_receipt_id']}',
                        extracted_at: '{edge['extracted_at']}',
                        inferred: {str(edge.get('inferred', False)).lower()},
                        confidence: {edge.get('confidence', 1.0)}
                    }}]->(to)
                    RETURN r
                $$) AS (r agtype);
                """

                await conn.execute(cypher_query)
                loaded_count += 1

            except Exception as e:
                logger.error(f"Error loading edge {edge['type']}: {e}")

        return loaded_count

    def _escape_cypher(self, text: str) -> str:
        """Escape special characters for Cypher queries"""
        if not text:
            return ""
        return text.replace("'", "\\'").replace('"', '\\"').replace('\n', '\\n')

    def extract_repo_from_path(self, file_path: str) -> str:
        """Extract repository name from file path"""
        if not file_path:
            return None

        # Handle paths like:
        # - "regen-network/regen-ledger/x/ecocredit/msg.go"  → "regen-ledger"
        # - "regen-ledger/x/ecocredit/msg.go"  → "regen-ledger"
        # - "x/ecocredit/msg.go" → None (no repo info)

        parts = file_path.split('/')

        # Check if any part matches our enabled repos
        for part in parts:
            if part in self.graph_enabled_repos:
                return part

        # Try to extract from org/repo pattern (first two parts)
        if len(parts) >= 2:
            potential_repo = parts[1] if parts[0] in ['regen-network', 'github.com'] else parts[0]
            if potential_repo in self.graph_enabled_repos:
                return potential_repo

        return None

    def close(self):
        """Close any open connections - called by service on shutdown"""
        # This processor doesn't maintain persistent connections
        # All connections are created and closed within process_event()
        logger.info("CodeGraphProcessor shutdown complete")
