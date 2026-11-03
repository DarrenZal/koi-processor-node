#!/usr/bin/env python3
"""
Infrastructure Registry Module
Allows infrastructure components to self-register in the unified knowledge graph
"""

import os
import json
import httpx
import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(title="Infrastructure Registry", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
FUSEKI_URL = os.getenv('FUSEKI_URL', 'http://localhost:3030')
FUSEKI_DATASET = os.getenv('FUSEKI_DATASET', 'koi')
FUSEKI_UPDATE_URL = f"{FUSEKI_URL}/{FUSEKI_DATASET}/update"
COORDINATOR_URL = os.getenv('COORDINATOR_URL', 'http://localhost:8005')

# Pydantic models
class ComponentRegistration(BaseModel):
    """Infrastructure component registration request"""
    component_id: str
    component_type: str  # processor, storage, service
    label: str
    endpoint: str
    port: Optional[int] = None
    description: Optional[str] = None
    capabilities: Optional[List[str]] = []
    depends_on: Optional[List[str]] = []  # Other component IDs
    metadata: Optional[Dict[str, Any]] = {}

class ComponentStatus(BaseModel):
    """Component status update"""
    component_id: str
    status: str  # active, idle, offline, error
    metrics: Optional[Dict[str, Any]] = {}
    last_heartbeat: Optional[str] = None

class ComponentConnection(BaseModel):
    """Component connection declaration"""
    source_id: str
    target_id: str
    connection_type: str  # data, control, query, monitoring

class InfrastructureRegistry:
    """Registry for infrastructure components"""

    def __init__(self):
        self.components: Dict[str, ComponentRegistration] = {}
        self.connections: List[ComponentConnection] = []
        self.status_cache: Dict[str, ComponentStatus] = {}

    async def register_component(self, registration: ComponentRegistration) -> bool:
        """
        Register an infrastructure component

        Args:
            registration: Component registration details

        Returns:
            bool: True if successful
        """
        try:
            # Store in memory
            self.components[registration.component_id] = registration

            # Generate RID
            rid = f"koi.infrastructure:{registration.component_id}"

            # Write to RDF graph
            await self._write_to_rdf(registration, rid)

            # Notify coordinator
            await self._notify_coordinator(registration)

            logger.info(f"Registered component: {registration.component_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to register component: {e}")
            return False

    async def update_status(self, status: ComponentStatus) -> bool:
        """Update component status"""
        try:
            self.status_cache[status.component_id] = status

            # Update RDF with status
            await self._update_status_in_rdf(status)

            return True
        except Exception as e:
            logger.error(f"Failed to update status: {e}")
            return False

    async def declare_connection(self, connection: ComponentConnection) -> bool:
        """Declare a connection between components"""
        try:
            self.connections.append(connection)

            # Write connection to RDF
            await self._write_connection_to_rdf(connection)

            return True
        except Exception as e:
            logger.error(f"Failed to declare connection: {e}")
            return False

    async def _write_to_rdf(self, registration: ComponentRegistration, rid: str) -> bool:
        """Write component registration to RDF graph"""
        try:
            # Map component type to RDF class
            rdf_class = {
                "processor": "koi:Processor",
                "storage": "koi:Storage",
                "service": "koi:Service"
            }.get(registration.component_type, "koi:Component")

            insert_query = f"""
            PREFIX koi: <http://koi.regen.network/ontology#>
            PREFIX infra: <http://koi.regen.network/infrastructure/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX dc: <http://purl.org/dc/elements/1.1/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

            INSERT DATA {{
                <infra:{registration.component_id}> a {rdf_class} ;
                    rdfs:label "{registration.label}" ;
                    koi:rid "{rid}" ;
                    koi:endpoint "{registration.endpoint}" ;
                    koi:componentType "{registration.component_type}" ;
                    koi:status "active" ;
                    koi:registeredAt "{datetime.now(timezone.utc).isoformat()}"^^xsd:dateTime .
            """

            if registration.port:
                insert_query += f'\n    <infra:{registration.component_id}> koi:port {registration.port} .'

            if registration.description:
                insert_query += f'\n    <infra:{registration.component_id}> dc:description "{registration.description}" .'

            # Add capabilities
            for capability in registration.capabilities or []:
                insert_query += f'\n    <infra:{registration.component_id}> koi:hasCapability "{capability}" .'

            # Add dependencies
            for dep in registration.depends_on or []:
                insert_query += f'\n    <infra:{registration.component_id}> koi:dependsOn <infra:{dep}> .'

            insert_query += "\n}"

            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    FUSEKI_UPDATE_URL,
                    data=insert_query,
                    headers={"Content-Type": "application/sparql-update"}
                )

                return response.status_code in [200, 204]

        except Exception as e:
            logger.error(f"Failed to write to RDF: {e}")
            return False

    async def _update_status_in_rdf(self, status: ComponentStatus) -> bool:
        """Update component status in RDF"""
        try:
            update_query = f"""
            PREFIX koi: <http://koi.regen.network/ontology#>
            PREFIX infra: <http://koi.regen.network/infrastructure/>
            PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

            DELETE {{ <infra:{status.component_id}> koi:status ?oldStatus }}
            INSERT {{ <infra:{status.component_id}> koi:status "{status.status}" }}
            WHERE {{ OPTIONAL {{ <infra:{status.component_id}> koi:status ?oldStatus }} }}
            """

            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    FUSEKI_UPDATE_URL,
                    data=update_query,
                    headers={"Content-Type": "application/sparql-update"}
                )

                return response.status_code in [200, 204]

        except Exception as e:
            logger.error(f"Failed to update status in RDF: {e}")
            return False

    async def _write_connection_to_rdf(self, connection: ComponentConnection) -> bool:
        """Write component connection to RDF"""
        try:
            # Map connection type to RDF property
            rdf_property = {
                "data": "koi:sendsTo",
                "control": "koi:controls",
                "query": "koi:queries",
                "monitoring": "koi:monitors"
            }.get(connection.connection_type, "koi:connectedTo")

            insert_query = f"""
            PREFIX koi: <http://koi.regen.network/ontology#>
            PREFIX infra: <http://koi.regen.network/infrastructure/>

            INSERT DATA {{
                <infra:{connection.source_id}> {rdf_property} <infra:{connection.target_id}> .
            }}
            """

            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    FUSEKI_UPDATE_URL,
                    data=insert_query,
                    headers={"Content-Type": "application/sparql-update"}
                )

                return response.status_code in [200, 204]

        except Exception as e:
            logger.error(f"Failed to write connection to RDF: {e}")
            return False

    async def _notify_coordinator(self, registration: ComponentRegistration) -> bool:
        """Notify coordinator of new component"""
        try:
            # Convert to coordinator-compatible format
            component_data = {
                "id": registration.component_id,
                "type": registration.component_type,
                "name": registration.label,
                "endpoint": registration.endpoint,
                "status": "active",
                "metadata": registration.metadata
            }

            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(
                    f"{COORDINATOR_URL}/infrastructure/register",
                    json=component_data
                )

                return response.status_code in [200, 201]

        except Exception as e:
            logger.warning(f"Could not notify coordinator: {e}")
            return False

# Global registry instance
registry = InfrastructureRegistry()

# API Endpoints

@app.post("/register")
async def register_component(registration: ComponentRegistration):
    """Register an infrastructure component"""
    success = await registry.register_component(registration)

    if success:
        return {
            "status": "success",
            "component_id": registration.component_id,
            "rid": f"koi.infrastructure:{registration.component_id}"
        }
    else:
        raise HTTPException(status_code=500, detail="Registration failed")

@app.post("/status")
async def update_component_status(status: ComponentStatus):
    """Update component status"""
    success = await registry.update_status(status)

    if success:
        return {"status": "success", "component_id": status.component_id}
    else:
        raise HTTPException(status_code=500, detail="Status update failed")

@app.post("/connect")
async def declare_connection(connection: ComponentConnection):
    """Declare a connection between components"""
    success = await registry.declare_connection(connection)

    if success:
        return {
            "status": "success",
            "source": connection.source_id,
            "target": connection.target_id
        }
    else:
        raise HTTPException(status_code=500, detail="Connection declaration failed")

@app.get("/components")
async def list_components():
    """List all registered components"""
    return {
        "components": [
            {
                "id": comp.component_id,
                "type": comp.component_type,
                "label": comp.label,
                "endpoint": comp.endpoint,
                "status": registry.status_cache.get(comp.component_id, {}).status
                if comp.component_id in registry.status_cache else "unknown"
            }
            for comp in registry.components.values()
        ]
    }

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "components_registered": len(registry.components),
        "connections_declared": len(registry.connections)
    }

# Example client code for components to self-register
class InfrastructureClient:
    """Client for infrastructure components to self-register"""

    def __init__(self, registry_url: str = "http://localhost:8003"):
        self.registry_url = registry_url

    async def register(
        self,
        component_id: str,
        component_type: str,
        label: str,
        endpoint: str,
        port: Optional[int] = None,
        **kwargs
    ) -> bool:
        """Register this component"""
        try:
            registration = {
                "component_id": component_id,
                "component_type": component_type,
                "label": label,
                "endpoint": endpoint,
                "port": port,
                **kwargs
            }

            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(
                    f"{self.registry_url}/register",
                    json=registration
                )

                return response.status_code == 200

        except Exception as e:
            logger.error(f"Failed to register: {e}")
            return False

    async def heartbeat(self, component_id: str, status: str = "active") -> bool:
        """Send heartbeat status update"""
        try:
            status_update = {
                "component_id": component_id,
                "status": status,
                "last_heartbeat": datetime.now(timezone.utc).isoformat()
            }

            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(
                    f"{self.registry_url}/status",
                    json=status_update
                )

                return response.status_code == 200

        except Exception as e:
            logger.error(f"Failed to send heartbeat: {e}")
            return False

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8003)