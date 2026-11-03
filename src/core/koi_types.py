"""
KOI Type Definitions
Based on the Knowledge Organization Infrastructure specification
"""

from enum import Enum
from typing import Dict, List, Any, Optional
from dataclasses import dataclass

class NodeType(Enum):
    """KOI Node Types"""
    FULL = "full"
    PROCESSOR = "processor"
    STORAGE = "storage"
    GATEWAY = "gateway"

class EventType(Enum):
    """KOI Event Types"""
    # FUN Events
    FORGET = "forget"
    UPDATE = "update"
    NEW = "new"
    
    # Processing Events
    RAG_QUERY = "rag_query"
    EMBEDDING_GENERATED = "embedding_generated"
    KNOWLEDGE_FRAGMENT_CREATED = "knowledge_fragment_created"
    
    # System Events
    NODE_INITIALIZED = "node_initialized"
    NODE_SHUTDOWN = "node_shutdown"
    SYNC_REQUESTED = "sync_requested"

@dataclass
class RID:
    """Resource Identifier"""
    namespace: str  # e.g., "orn" for Omnipresent Resource Name
    type: str       # e.g., "regen", "koi", "cat"
    identifier: str # Unique identifier
    
    def __str__(self):
        return f"{self.namespace}:{self.type}.{self.identifier}"
    
    @classmethod
    def from_string(cls, rid_string: str):
        parts = rid_string.split(":")
        if len(parts) != 2:
            raise ValueError(f"Invalid RID format: {rid_string}")
        namespace = parts[0]
        type_and_id = parts[1].split(".", 1)
        if len(type_and_id) != 2:
            raise ValueError(f"Invalid RID format: {rid_string}")
        return cls(namespace, type_and_id[0], type_and_id[1])

@dataclass
class Event:
    """KOI Event"""
    rid: str
    event_type: str
    timestamp: str
    source_node: str
    data: Dict[str, Any]
    target_nodes: Optional[List[str]] = None
    parent_event: Optional[str] = None

@dataclass
class FUNState:
    """Forget-Update-New State"""
    state_rid: str
    version: int
    timestamp: str
    data: Dict[str, Any]
    previous_version: Optional[str] = None
    
    def forget(self, fields: List[str]) -> 'FUNState':
        """Create new state with fields forgotten"""
        new_data = {k: v for k, v in self.data.items() if k not in fields}
        return FUNState(
            state_rid=self.state_rid,
            version=self.version + 1,
            timestamp=self.timestamp,
            data=new_data,
            previous_version=f"{self.state_rid}:v{self.version}"
        )
    
    def update(self, updates: Dict[str, Any]) -> 'FUNState':
        """Create new state with updates"""
        new_data = {**self.data, **updates}
        return FUNState(
            state_rid=self.state_rid,
            version=self.version + 1,
            timestamp=self.timestamp,
            data=new_data,
            previous_version=f"{self.state_rid}:v{self.version}"
        )
    
    def new(self, data: Dict[str, Any]) -> 'FUNState':
        """Create entirely new state"""
        return FUNState(
            state_rid=self.state_rid,
            version=self.version + 1,
            timestamp=self.timestamp,
            data=data,
            previous_version=f"{self.state_rid}:v{self.version}"
        )

@dataclass
class CATReceipt:
    """Content Addressable Transformation Receipt"""
    receipt_id: str
    timestamp: str
    operation: str
    input_rids: List[str]
    output_rids: List[str]
    metadata: Dict[str, Any]
    signature: Optional[str] = None
    
    def verify(self) -> bool:
        """Verify the integrity of the CAT receipt"""
        # In production, this would verify cryptographic signatures
        return True

@dataclass
class KnowledgeFragment:
    """A fragment of knowledge in the KOI system"""
    rid: str
    content: str
    content_hash: str  # SHA-256 hash for deduplication
    source: str
    chunk_index: int
    metadata: Dict[str, Any]
    embedding_rid: Optional[str] = None
    relationships: Optional[List[str]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "rid": self.rid,
            "content": self.content,
            "content_hash": self.content_hash,
            "source": self.source,
            "chunk_index": self.chunk_index,
            "metadata": self.metadata,
            "embedding_rid": self.embedding_rid,
            "relationships": self.relationships or []
        }

@dataclass
class KOINode:
    """KOI Node Configuration"""
    node_rid: str
    node_name: str
    node_type: NodeType
    provides: Dict[str, List[str]]  # What events/states this node provides
    requires: Dict[str, List[str]]  # What events/states this node requires
    endpoints: Dict[str, str]       # Service endpoints
    metadata: Dict[str, Any]
    
    def can_provide(self, event_type: str) -> bool:
        """Check if this node can provide a specific event type"""
        return event_type in self.provides.get("event", [])
    
    def requires_event(self, event_type: str) -> bool:
        """Check if this node requires a specific event type"""
        return event_type in self.requires.get("event", [])