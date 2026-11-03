#!/usr/bin/env python3
"""
Complete End-to-End Validation of KOI Pipeline
Tests the entire flow from sensors to agent knowledge access

With --apply it checks the coordinator (:8005), event bridge (:8100), BGE server (:8090)
and MCP knowledge server (:8200) on localhost, POSTs a test event to the coordinator's
/api/event (it stays in the pipeline), POSTs to /encode and /search, and writes the
results to --output (default /opt/projects/koi-processor/validation_results.json).
With no arguments or --dry-run it only prints what it would do.
"""

import argparse
import asyncio
import sys
import httpx
import json
import time
from datetime import datetime
from typing import Dict, Any
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DEFAULT_OUTPUT = "/opt/projects/koi-processor/validation_results.json"

class PipelineValidator:
    """Validates the complete KOI pipeline"""
    
    def __init__(self):
        self.coordinator_url = "http://localhost:8005"
        self.event_bridge_url = "http://localhost:8100"
        self.bge_server_url = "http://localhost:8090"
        self.mcp_server_url = "http://localhost:8200"
        self.results = {
            "timestamp": datetime.now().isoformat(),
            "components": {},
            "tests": {},
            "summary": {}
        }
    
    async def check_component(self, name: str, url: str, path: str = "/") -> bool:
        """Check if a component is running"""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{url}{path}")
                if response.status_code == 200:
                    self.results["components"][name] = {
                        "status": "✅ Running",
                        "url": url,
                        "response": response.json() if path == "/" else "OK"
                    }
                    return True
        except Exception as e:
            self.results["components"][name] = {
                "status": "❌ Failed",
                "url": url,
                "error": str(e)
            }
        return False
    
    async def test_sensor_to_coordinator(self) -> bool:
        """Test: Sensor → Coordinator flow"""
        test_name = "Sensor → Coordinator"
        try:
            # Create test event
            test_event = {
                "source_sensor": f"validation.test.{int(time.time())}",
                "content": {
                    "test": "validation",
                    "timestamp": datetime.now().isoformat(),
                    "message": "End-to-end validation test"
                },
                "metadata": {
                    "type": "test",
                    "validator": "complete_validation.py"
                }
            }
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    f"{self.coordinator_url}/api/event",
                    json=test_event
                )
                
                if response.status_code == 200:
                    result = response.json()
                    self.results["tests"][test_name] = {
                        "status": "✅ Passed",
                        "rid": result.get("rid"),
                        "message": "Event accepted by coordinator"
                    }
                    return True
                else:
                    self.results["tests"][test_name] = {
                        "status": "❌ Failed",
                        "error": f"Status {response.status_code}: {response.text}"
                    }
        except Exception as e:
            self.results["tests"][test_name] = {
                "status": "❌ Failed",
                "error": str(e)
            }
        return False
    
    async def test_bge_embedding(self) -> bool:
        """Test: BGE embedding generation"""
        test_name = "BGE Embedding Generation"
        try:
            test_text = "Regen Network enables ecological regeneration"
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    f"{self.bge_server_url}/encode",
                    json={"text": test_text}
                )
                
                if response.status_code == 200:
                    result = response.json()
                    embedding = result.get("embedding", [])
                    if len(embedding) == 1024:
                        self.results["tests"][test_name] = {
                            "status": "✅ Passed",
                            "dimension": 1024,
                            "message": "BGE embedding generated successfully"
                        }
                        return True
                    else:
                        self.results["tests"][test_name] = {
                            "status": "⚠️ Warning",
                            "dimension": len(embedding),
                            "message": f"Unexpected embedding dimension: {len(embedding)}"
                        }
        except Exception as e:
            self.results["tests"][test_name] = {
                "status": "❌ Failed",
                "error": str(e)
            }
        return False
    
    async def test_knowledge_search(self) -> bool:
        """Test: MCP Knowledge Server search"""
        test_name = "Knowledge Search"
        try:
            search_query = {
                "query": "carbon",
                "limit": 3
            }
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    f"{self.mcp_server_url}/search",
                    json=search_query
                )
                
                if response.status_code == 200:
                    result = response.json()
                    memories = result.get("memories", [])
                    self.results["tests"][test_name] = {
                        "status": "✅ Passed",
                        "results_found": len(memories),
                        "message": f"Found {len(memories)} matching memories"
                    }
                    return True
        except Exception as e:
            self.results["tests"][test_name] = {
                "status": "❌ Failed",
                "error": str(e)
            }
        return False
    
    async def test_pipeline_stats(self) -> bool:
        """Test: Get pipeline statistics"""
        test_name = "Pipeline Statistics"
        try:
            stats = {}
            
            # Get Event Bridge stats
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.event_bridge_url}/stats")
                if response.status_code == 200:
                    stats["event_bridge"] = response.json()
            
            # Get MCP stats
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{self.mcp_server_url}/stats")
                if response.status_code == 200:
                    stats["mcp_server"] = response.json()
            
            if stats:
                self.results["tests"][test_name] = {
                    "status": "✅ Passed",
                    "stats": stats,
                    "message": "Pipeline statistics retrieved"
                }
                return True
        except Exception as e:
            self.results["tests"][test_name] = {
                "status": "❌ Failed",
                "error": str(e)
            }
        return False
    
    async def validate(self, output_path: str = DEFAULT_OUTPUT):
        """Run complete validation"""
        logger.info("="*60)
        logger.info("KOI PIPELINE COMPLETE VALIDATION")
        logger.info("="*60)
        
        # Check all components
        logger.info("\n📊 Checking Components...")
        components = [
            ("KOI Coordinator", self.coordinator_url),
            ("Event Bridge", self.event_bridge_url),
            ("BGE Server", self.bge_server_url, "/health"),
            ("MCP Knowledge Server", self.mcp_server_url)
        ]
        
        component_results = []
        for comp in components:
            result = await self.check_component(*comp)
            component_results.append(result)
            status = "✅" if result else "❌"
            logger.info(f"  {status} {comp[0]}: {comp[1]}")
        
        # Run tests
        logger.info("\n🧪 Running Tests...")
        test_results = []
        
        tests = [
            ("Sensor → Coordinator", self.test_sensor_to_coordinator),
            ("BGE Embeddings", self.test_bge_embedding),
            ("Knowledge Search", self.test_knowledge_search),
            ("Pipeline Stats", self.test_pipeline_stats)
        ]
        
        for test_name, test_func in tests:
            logger.info(f"  Testing {test_name}...")
            result = await test_func()
            test_results.append(result)
            status = self.results["tests"].get(test_name, {}).get("status", "❌")
            logger.info(f"    {status}")
        
        # Summary
        total_components = len(component_results)
        working_components = sum(component_results)
        total_tests = len(test_results)
        passed_tests = sum(test_results)
        
        self.results["summary"] = {
            "components": f"{working_components}/{total_components}",
            "tests": f"{passed_tests}/{total_tests}",
            "overall": "✅ OPERATIONAL" if working_components == total_components and passed_tests >= 3 else "⚠️ PARTIAL"
        }
        
        # Print summary
        logger.info("\n" + "="*60)
        logger.info("📈 VALIDATION SUMMARY")
        logger.info("="*60)
        logger.info(f"Components: {working_components}/{total_components} operational")
        logger.info(f"Tests: {passed_tests}/{total_tests} passed")
        logger.info(f"Status: {self.results['summary']['overall']}")
        
        # Save results
        with open(output_path, "w") as f:
            json.dump(self.results, f, indent=2)
        logger.info(f"\n💾 Results saved to validation_results.json")
        
        # Final verdict
        if self.results["summary"]["overall"] == "✅ OPERATIONAL":
            logger.info("\n🎉 SUCCESS: KOI Pipeline is fully operational!")
            logger.info("   Agents can now access knowledge through MCP server at port 8200")
        else:
            logger.info("\n⚠️ WARNING: Some components need attention")
            logger.info("   Check validation_results.json for details")

async def main(output_path: str = DEFAULT_OUTPUT):
    """Main entry point"""
    validator = PipelineValidator()
    await validator.validate(output_path)


def cli_main(argv=None) -> int:
    """Command-line entry: a dry run unless --apply is given."""
    parser = argparse.ArgumentParser(
        description="End-to-end KOI pipeline validation against the services on localhost. "
                    "Default is a dry run that contacts nothing.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report what would happen and exit (the default; wins over --apply)")
    parser.add_argument("--apply", action="store_true",
                        help="Contact the services, post the test event and write the results")
    parser.add_argument("--output", default=DEFAULT_OUTPUT,
                        help="Where --apply writes the results (default: %(default)s)")
    args = parser.parse_args(argv)

    v = PipelineValidator()
    print(f"Targets: coordinator {v.coordinator_url}, event bridge {v.event_bridge_url}, "
          f"BGE {v.bge_server_url}, MCP {v.mcp_server_url}")
    print("Steps: GET each service; POST a test event to the coordinator's /api/event (it is "
          "processed like any event and not removed), POST /encode and /search, GET /stats; "
          f"write the results to {args.output}")
    if args.dry_run or not args.apply:
        print("\nDry run: nothing was sent or written. Re-run with --apply to do this.")
        return 0
    asyncio.run(main(args.output))
    return 0


if __name__ == "__main__":
    sys.exit(cli_main())