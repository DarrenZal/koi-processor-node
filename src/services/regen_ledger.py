#!/usr/bin/env python3
"""
Regen Ledger Data Source
Fetches on-chain data from Regen Network blockchain for content curation
Now uses comprehensive client for ALL available data
"""

import aiohttp
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta, timezone
from loguru import logger
import json
from .regen_ledger_comprehensive import RegenLedgerComprehensive

class RegenLedgerClient:
    """
    Client for fetching data from Regen Network blockchain
    Using the same RPC endpoints as the MCP server
    """

    def __init__(self, rpc_endpoint: str = "https://regen-rpc.polkachu.com"):
        """
        Initialize Regen ledger client - now using comprehensive client

        Args:
            rpc_endpoint: RPC endpoint URL for Regen Network
        """
        # Initialize the comprehensive client internally
        self.comprehensive_client = RegenLedgerComprehensive(rpc_endpoint)
        self.rpc_endpoint = rpc_endpoint.rstrip('/')
        self.rest_endpoint = "https://regen-rest.publicnode.com"
        logger.info(f"Initialized Regen ledger client (comprehensive mode) with REST endpoint: {self.rest_endpoint}")

    async def _make_request(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make HTTP request to Regen REST API

        Args:
            path: API path (e.g., '/cosmos/gov/v1/proposals')
            params: Query parameters

        Returns:
            JSON response data
        """
        url = f"{self.rest_endpoint}{path}"

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.error(f"Request failed: {response.status} - {await response.text()}")
                        return {}
            except Exception as e:
                logger.error(f"Error making request to {url}: {e}")
                return {}

    async def get_recent_proposals(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent governance proposals

        Args:
            limit: Maximum number of proposals to return

        Returns:
            List of proposal data
        """
        logger.info("Fetching recent governance proposals...")

        # Fetch proposals with VOTING_PERIOD and PASSED status
        statuses = ["PROPOSAL_STATUS_VOTING_PERIOD", "PROPOSAL_STATUS_PASSED", "PROPOSAL_STATUS_REJECTED"]
        all_proposals = []

        for status in statuses:
            data = await self._make_request(
                "/cosmos/gov/v1/proposals",
                params={
                    "proposal_status": status,
                    "pagination.limit": str(limit),
                    "pagination.reverse": "true"
                }
            )

            if data and "proposals" in data:
                proposals = data["proposals"]
                for proposal in proposals:
                    # Parse and format proposal data
                    formatted = {
                        "id": proposal.get("id"),
                        "title": proposal.get("title", ""),
                        "summary": proposal.get("summary", ""),
                        "status": proposal.get("status", "").replace("PROPOSAL_STATUS_", ""),
                        "submit_time": proposal.get("submit_time"),
                        "voting_end_time": proposal.get("voting_end_time"),
                        "type": "governance_proposal"
                    }
                    all_proposals.append(formatted)

        # Sort by submit time and return most recent
        all_proposals.sort(key=lambda x: x.get("submit_time", ""), reverse=True)
        return all_proposals[:limit]

    async def get_recent_credit_batches(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent ecocredit batches

        Args:
            limit: Maximum number of batches to return

        Returns:
            List of credit batch data
        """
        logger.info("Fetching recent credit batches...")

        data = await self._make_request(
            "/regen/ecocredit/v1/batches",
            params={
                "pagination.limit": str(limit),
                "pagination.reverse": "true"
            }
        )

        if not data or "batches" not in data:
            return []

        batches = []
        for batch in data["batches"]:
            formatted = {
                "denom": batch.get("denom", ""),
                "metadata": batch.get("metadata", ""),
                "start_date": batch.get("start_date"),
                "end_date": batch.get("end_date"),
                "issuance_date": batch.get("issuance_date"),
                "type": "credit_batch"
            }
            batches.append(formatted)

        return batches

    async def get_recent_credit_classes(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent credit classes

        Args:
            limit: Maximum number of classes to return

        Returns:
            List of credit class data
        """
        logger.info("Fetching recent credit classes...")

        data = await self._make_request(
            "/regen/ecocredit/v1/classes",
            params={
                "pagination.limit": str(limit),
                "pagination.reverse": "true"
            }
        )

        if not data or "classes" not in data:
            return []

        classes = []
        for cls in data["classes"]:
            formatted = {
                "id": cls.get("id", ""),
                "admin": cls.get("admin", ""),
                "metadata": cls.get("metadata", ""),
                "credit_type": cls.get("credit_type", {}).get("abbreviation", ""),
                "type": "credit_class"
            }
            classes.append(formatted)

        return classes

    async def get_marketplace_activity(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent marketplace sell orders

        Args:
            limit: Maximum number of orders to return

        Returns:
            List of sell order data
        """
        logger.info("Fetching marketplace activity...")

        data = await self._make_request(
            "/regen/ecocredit/marketplace/v1/sell-orders",
            params={
                "pagination.limit": str(limit),
                "pagination.reverse": "true"
            }
        )

        if not data or "sell_orders" not in data:
            return []

        orders = []
        for order in data["sell_orders"]:
            formatted = {
                "id": order.get("id"),
                "seller": order.get("seller", ""),
                "batch_denom": order.get("batch_denom", ""),
                "quantity": order.get("quantity", ""),
                "ask_price": order.get("ask_price", {}).get("amount", ""),
                "ask_denom": order.get("ask_price", {}).get("denom", ""),
                "disable_auto_retire": order.get("disable_auto_retire", False),
                "type": "marketplace_order"
            }
            orders.append(formatted)

        return orders

    async def get_basket_activity(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get basket token activity

        Args:
            limit: Maximum number of baskets to return

        Returns:
            List of basket data
        """
        logger.info("Fetching basket activity...")

        data = await self._make_request(
            "/regen/ecocredit/basket/v1/baskets",
            params={
                "pagination.limit": str(limit)
            }
        )

        if not data or "baskets" not in data:
            return []

        baskets = []
        for basket in data["baskets"]:
            formatted = {
                "denom": basket.get("basket_denom", ""),
                "name": basket.get("name", ""),
                "credit_type": basket.get("credit_type_abbrev", ""),
                "date_created": basket.get("date_created"),
                "type": "basket_token"
            }
            baskets.append(formatted)

        return baskets

    async def get_ledger_summary(self) -> Dict[str, Any]:
        """
        Get a COMPREHENSIVE summary of recent ledger activity
        Now uses the comprehensive client to fetch ALL available data

        Returns:
            Dictionary containing ALL types of ledger data
        """
        logger.info("Fetching comprehensive ledger summary (24h)...")

        # Get comprehensive 24-hour summary
        comprehensive_summary = await self.comprehensive_client.get_comprehensive_24h_summary()

        # Transform to match expected format but with much more data
        summary = {
            "timestamp": comprehensive_summary.get("timestamp"),
            "proposals": comprehensive_summary.get("proposals", []),
            "credit_batches": comprehensive_summary.get("credit_batches", []),
            "credit_classes": [],  # We'll fetch these separately if needed
            "marketplace_orders": comprehensive_summary.get("marketplace", {}).get("sell_orders", []),
            "buy_orders": comprehensive_summary.get("marketplace", {}).get("buy_orders", []),
            "baskets": comprehensive_summary.get("baskets", []),
            "staking": comprehensive_summary.get("staking", {}),
            "token_supply": comprehensive_summary.get("token_supply", {}),
            "recent_blocks": comprehensive_summary.get("recent_blocks", []),
            "ibc": comprehensive_summary.get("ibc", {}),
            "statistics": comprehensive_summary.get("statistics", {})
        }

        # Add backward-compatible statistics
        stats = comprehensive_summary.get("statistics", {})
        summary["statistics"].update({
            "total_proposals": stats.get("governance", {}).get("total_proposals", 0),
            "active_proposals": stats.get("governance", {}).get("active_proposals", 0),
            "total_credit_batches": stats.get("ecocredit", {}).get("new_batches", 0),
            "total_credit_classes": len(summary.get("credit_classes", [])),
            "active_sell_orders": stats.get("marketplace", {}).get("active_sell_orders", 0),
            "active_buy_orders": stats.get("marketplace", {}).get("active_buy_orders", 0),
            "total_baskets": stats.get("ecocredit", {}).get("total_baskets", 0),
            "total_validators": stats.get("staking", {}).get("total_validators", 0),
            "total_bonded": stats.get("staking", {}).get("total_bonded", "0"),
            "ibc_channels": stats.get("network", {}).get("ibc_channels", 0),
            "avg_tx_per_block": stats.get("network", {}).get("avg_tx_per_block", 0)
        })

        # Include raw data for block explorer links
        summary['raw_data'] = {
            'recent_blocks': comprehensive_summary.get('recent_blocks', []),
            'marketplace': comprehensive_summary.get('marketplace', {}),
            'proposals': comprehensive_summary.get('proposals', [])
        }

        logger.info(f"Comprehensive ledger summary fetched: {summary['statistics']}")
        return summary

    async def format_for_daily_post(self) -> str:
        """
        Format COMPREHENSIVE ledger data for daily social media posts

        Returns:
            Formatted text with ALL available on-chain activity
        """
        summary = await self.get_ledger_summary()
        stats = summary.get("statistics", {})

        lines = ["📊 **Regen Network 24h On-Chain Update**\n"]

        # Governance updates
        if stats.get("active_proposals", 0) > 0:
            lines.append(f"🗳️ {stats['active_proposals']} active governance proposal(s)")
            for prop in summary.get("proposals", [])[:2]:
                if prop.get("status") == "VOTING_PERIOD":
                    lines.append(f"  • #{prop['id']}: {prop['title'][:40]}...")

        # Credit and marketplace activity
        if stats.get("total_credit_batches", 0) > 0:
            lines.append(f"🌱 {stats['total_credit_batches']} new credit batches")

        sell_orders = stats.get("active_sell_orders", 0)
        buy_orders = stats.get("active_buy_orders", 0)
        if sell_orders > 0 or buy_orders > 0:
            lines.append(f"💱 Marketplace: {sell_orders} sell / {buy_orders} buy orders")

        # Network statistics
        validators = stats.get("total_validators", 0)
        if validators > 0:
            lines.append(f"🔒 {validators} active validators")

        avg_tx = stats.get("avg_tx_per_block", 0)
        if avg_tx > 0:
            lines.append(f"⛓️ Network activity: {avg_tx:.1f} tx/block avg")

        # IBC activity
        ibc_channels = stats.get("ibc_channels", 0)
        if ibc_channels > 0:
            lines.append(f"🌉 {ibc_channels} IBC channels active")

        # Staking info
        total_bonded = stats.get("total_bonded", "0")
        if total_bonded and total_bonded != "0":
            # Convert to REGEN from uREGEN (divide by 1e6)
            try:
                bonded_regen = int(total_bonded) / 1e6
                lines.append(f"💰 {bonded_regen:,.0f} REGEN bonded")
            except:
                pass

        # Basket tokens
        if summary.get("baskets"):
            basket_names = [b.get("name", "") for b in summary["baskets"][:3] if b.get("name")]
            if basket_names:
                lines.append(f"🧺 Active baskets: {', '.join(basket_names)}")

        return "\n".join(lines)

    async def format_for_weekly_digest(self) -> Dict[str, Any]:
        """
        Format COMPREHENSIVE ledger data for weekly digest

        Returns:
            Structured data with ALL on-chain activity for weekly digest
        """
        # Get 7-day comprehensive summary
        comprehensive_summary = await self.comprehensive_client.get_comprehensive_weekly_summary()

        # Use the comprehensive format method
        digest_data = self.comprehensive_client.format_comprehensive_weekly_digest(comprehensive_summary)

        # Add a formatted summary for the digest - use WEEKLY format, not daily!
        digest_data["summary"] = self.comprehensive_client.format_comprehensive_weekly_post(comprehensive_summary)

        return digest_data


# Test function
async def test_ledger_client():
    """Test the Regen ledger client"""
    client = RegenLedgerClient()

    print("Testing Regen Ledger Client...")
    print("-" * 50)

    # Test daily post format
    print("\nDaily Post Format:")
    daily_content = await client.format_for_daily_post()
    print(daily_content)

    print("\n" + "-" * 50)

    # Test weekly digest format
    print("\nWeekly Digest Data:")
    weekly_data = await client.format_for_weekly_digest()
    print(json.dumps(weekly_data, indent=2, default=str))

    print("\n" + "-" * 50)

    # Test full summary
    print("\nFull Ledger Summary:")
    summary = await client.get_ledger_summary()
    print(f"Statistics: {summary['statistics']}")


if __name__ == "__main__":
    asyncio.run(test_ledger_client())