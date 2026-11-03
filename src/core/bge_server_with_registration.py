#!/usr/bin/env python3
"""
Example: BGE Server with Self-Registration
Shows how infrastructure components can self-register

With --run it registers the BGE server with the infrastructure registry at
http://localhost:8003 and then sends a heartbeat every 30 seconds, forever. With no
arguments or --dry-run it only prints what it would do.
"""

import argparse
import asyncio
import sys
import logging
from infrastructure_registry import InfrastructureClient

logger = logging.getLogger(__name__)

async def register_bge_server():
    """Register BGE server with infrastructure registry"""

    # Create registry client
    client = InfrastructureClient(registry_url="http://localhost:8003")

    # Register BGE server
    success = await client.register(
        component_id="bge-embeddings",
        component_type="processor",
        label="BGE Embedding Server",
        endpoint="http://localhost:8090",
        port=8090,
        description="Generates BAAI/bge-large-en-v1.5 1024-dimensional embeddings",
        capabilities=[
            "text-embedding",
            "bge-large-en-v1.5",
            "1024-dimensions",
            "batch-processing"
        ],
        depends_on=["event-bridge"],  # Depends on Event Bridge for input
        metadata={
            "model": "BAAI/bge-large-en-v1.5",
            "dimensions": 1024,
            "max_batch_size": 32,
            "version": "1.0.0"
        }
    )

    if success:
        logger.info("BGE server registered successfully")

        # Start heartbeat loop
        while True:
            await asyncio.sleep(30)  # Send heartbeat every 30 seconds
            await client.heartbeat("bge-embeddings", status="active")
    else:
        logger.error("Failed to register BGE server")

def cli_main(argv=None) -> int:
    """Command-line entry: a dry run unless --run is given."""
    parser = argparse.ArgumentParser(
        description="Register the BGE server with the infrastructure registry and send "
                    "heartbeats. Default is a dry run that contacts nothing.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would happen and exit (the default; wins over --run)")
    parser.add_argument("--run", action="store_true",
                        help="Register, then send a heartbeat every 30 s until stopped")
    args = parser.parse_args(argv)

    print("Registry: http://localhost:8003 (POST the 'bge-embeddings' registration, then a "
          "heartbeat every 30 s)")
    if args.dry_run or not args.run:
        print("\nDry run: nothing was sent. Re-run with --run to start.")
        return 0
    logging.basicConfig(level=logging.INFO)
    asyncio.run(register_bge_server())
    return 0


# This would be added to the actual BGE server startup code
if __name__ == "__main__":
    sys.exit(cli_main())
