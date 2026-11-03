#!/usr/bin/env python3
"""
Direct Regen Ledger Client
Queries Regen Network blockchain directly via RPC and REST endpoints
"""

import httpx
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from loguru import logger

class RegenDirectClient:
    """Direct client for Regen Network blockchain data"""

    def __init__(self):
        # Public endpoints
        self.rpc_endpoint = "https://regen-rpc.polkachu.com"
        self.rest_endpoint = "https://regen-mainnet-lcd.autostake.com:443"  # Alternative REST endpoint
        self.client = httpx.AsyncClient(timeout=30.0)

    async def get_ledger_stats(self) -> Dict[str, Any]:
        """Get comprehensive ledger statistics"""
        stats = {}

        try:
            # Get blockchain status
            status = await self._get_status()
            stats.update(status)

            # Get validator info
            validators = await self._get_validators()
            stats['validator_count'] = len(validators)

            # Get ecocredit stats
            ecocredit = await self._get_ecocredit_stats()
            stats.update(ecocredit)

            # Get governance info
            governance = await self._get_governance_stats()
            stats.update(governance)

        except Exception as e:
            logger.error(f"Error getting ledger stats: {e}")

        return stats

    async def _get_status(self) -> Dict[str, Any]:
        """Get blockchain status"""
        try:
            response = await self.client.get(f"{self.rpc_endpoint}/status")
            if response.status_code == 200:
                data = response.json()
                result = data.get('result', {})
                sync_info = result.get('sync_info', {})
                node_info = result.get('node_info', {})

                return {
                    'block_height': int(sync_info.get('latest_block_height', 0)),
                    'chain_id': node_info.get('network', 'regen-1'),
                    'latest_block_time': sync_info.get('latest_block_time', ''),
                    'catching_up': sync_info.get('catching_up', False)
                }
        except Exception as e:
            logger.error(f"Error getting status: {e}")

        return {'block_height': 0, 'chain_id': 'regen-1'}

    async def _get_validators(self) -> List[Dict]:
        """Get active validators"""
        try:
            response = await self.client.get(f"{self.rpc_endpoint}/validators")
            if response.status_code == 200:
                data = response.json()
                return data.get('result', {}).get('validators', [])
        except Exception as e:
            logger.error(f"Error getting validators: {e}")

        return []

    async def _get_ecocredit_stats(self) -> Dict[str, Any]:
        """Get ecocredit statistics"""
        stats = {
            'credit_classes': 0,
            'credit_batches': 0,
            'total_credits': 0
        }

        try:
            # Get credit classes
            response = await self.client.get(
                f"{self.rest_endpoint}/regen/ecocredit/v1/classes",
                params={'pagination.limit': 100}
            )
            if response.status_code == 200:
                data = response.json()
                classes = data.get('classes', [])
                stats['credit_classes'] = len(classes)

                # Get batch count for each class
                batch_count = 0
                for cls in classes[:5]:  # Sample first 5 classes
                    batch_resp = await self.client.get(
                        f"{self.rest_endpoint}/regen/ecocredit/v1/batches/class/{cls['id']}",
                        params={'pagination.limit': 1}
                    )
                    if batch_resp.status_code == 200:
                        batch_data = batch_resp.json()
                        pagination = batch_data.get('pagination', {})
                        total = int(pagination.get('total', 0))
                        batch_count += total

                # Estimate total batches
                if classes:
                    stats['credit_batches'] = batch_count * (len(classes) / 5)

        except Exception as e:
            logger.error(f"Error getting ecocredit stats: {e}")

        return stats

    async def _get_governance_stats(self) -> Dict[str, Any]:
        """Get governance statistics"""
        stats = {
            'active_proposals': 0,
            'total_proposals': 0
        }

        try:
            # Get active proposals (status=2 is voting period)
            response = await self.client.get(
                f"{self.rest_endpoint}/cosmos/gov/v1beta1/proposals",
                params={'proposal_status': 2}
            )
            if response.status_code == 200:
                data = response.json()
                proposals = data.get('proposals', [])
                stats['active_proposals'] = len(proposals)

            # Get all proposals
            response = await self.client.get(
                f"{self.rest_endpoint}/cosmos/gov/v1beta1/proposals"
            )
            if response.status_code == 200:
                data = response.json()
                stats['total_proposals'] = len(data.get('proposals', []))

        except Exception as e:
            logger.error(f"Error getting governance stats: {e}")

        return stats

    async def get_recent_activity(self, hours: int = 24) -> List[Dict[str, Any]]:
        """Get recent blockchain activity"""
        activities = []

        try:
            # Get recent proposals
            proposals = await self._get_recent_proposals(hours)
            activities.extend(proposals)

            # Get recent credit issuances (simplified - would need event monitoring)
            # For now, return sample data
            activities.append({
                'type': 'credit_issuance',
                'description': 'New carbon credits issued',
                'timestamp': datetime.now().isoformat()
            })

        except Exception as e:
            logger.error(f"Error getting recent activity: {e}")

        return activities

    async def _get_recent_proposals(self, hours: int) -> List[Dict[str, Any]]:
        """Get recent governance proposals"""
        activities = []

        try:
            response = await self.client.get(
                f"{self.rest_endpoint}/cosmos/gov/v1beta1/proposals"
            )
            if response.status_code == 200:
                data = response.json()
                proposals = data.get('proposals', [])

                cutoff = datetime.now() - timedelta(hours=hours)

                for prop in proposals:
                    # Check if proposal is recent (simplified check)
                    submit_time = prop.get('submit_time', '')
                    if submit_time:
                        try:
                            prop_time = datetime.fromisoformat(submit_time.replace('Z', '+00:00'))
                            if prop_time > cutoff:
                                activities.append({
                                    'type': 'governance_proposal',
                                    'proposal_id': prop.get('proposal_id'),
                                    'title': prop.get('content', {}).get('title', 'Unknown'),
                                    'status': prop.get('status'),
                                    'timestamp': submit_time
                                })
                        except:
                            pass

        except Exception as e:
            logger.error(f"Error getting recent proposals: {e}")

        return activities

    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()


async def test_direct_client():
    """Test the direct Regen client"""
    client = RegenDirectClient()

    try:
        # Get stats
        stats = await client.get_ledger_stats()
        logger.info(f"Ledger stats: {stats}")

        # Get recent activity
        activities = await client.get_recent_activity(24)
        logger.info(f"Recent activities: {len(activities)} items")

        return stats

    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(test_direct_client())