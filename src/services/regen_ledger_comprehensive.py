#!/usr/bin/env python3
"""
Comprehensive Regen Ledger Data Source
Fetches ALL available on-chain data from Regen Network blockchain for content curation
Includes all modules: governance, ecocredit, marketplace, basket, staking, bank, distribution, etc.
"""

import aiohttp
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta, timezone
from loguru import logger
import json

class RegenLedgerComprehensive:
    """
    Comprehensive client for fetching ALL data from Regen Network blockchain
    """

    def __init__(self, rpc_endpoint: str = "https://regen-rpc.polkachu.com"):
        """
        Initialize comprehensive Regen ledger client

        Args:
            rpc_endpoint: RPC endpoint URL for Regen Network
        """
        self.rpc_endpoint = rpc_endpoint.rstrip('/')
        self.rest_endpoint = "https://regen-rest.publicnode.com"
        logger.info(f"Initialized comprehensive Regen ledger client with REST endpoint: {self.rest_endpoint}")

    async def _make_request(self, path: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make HTTP request to Regen REST API

        Args:
            path: API path
            params: Query parameters

        Returns:
            JSON response data
        """
        url = f"{self.rest_endpoint}{path}"

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        logger.error(f"Request failed {path}: {response.status}")
                        return {}
            except Exception as e:
                logger.error(f"Error making request to {url}: {e}")
                return {}

    # ==================== GOVERNANCE MODULE ====================

    async def get_all_proposals(self, days_back: int = 7) -> List[Dict[str, Any]]:
        """
        Get ALL proposals from the specified time period
        """
        logger.info(f"Fetching all proposals from past {days_back} days...")

        # Get all proposals (no status filter since it doesn't seem to work)
        data = await self._make_request(
            "/cosmos/gov/v1/proposals",
            params={
                "pagination.limit": "100",
                "pagination.reverse": "true"
            }
        )

        all_proposals = []
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)

        if data and "proposals" in data:
            for proposal in data["proposals"]:
                # Check if proposal is within our time window (either submit or voting time)
                submit_time = proposal.get("submit_time", "")
                voting_end_time = proposal.get("voting_end_time", "")

                relevant = False
                if voting_end_time:
                    voting_end_dt = datetime.fromisoformat(voting_end_time.replace('Z', '+00:00'))
                    if voting_end_dt >= cutoff_date:
                        relevant = True

                if submit_time and not relevant:
                    submit_dt = datetime.fromisoformat(submit_time.replace('Z', '+00:00'))
                    if submit_dt >= cutoff_date:
                        relevant = True

                if relevant:
                    formatted = {
                        "id": proposal.get("id"),
                        "title": proposal.get("title", ""),
                        "summary": proposal.get("summary", ""),
                        "status": proposal.get("status", "").replace("PROPOSAL_STATUS_", ""),
                        "submit_time": submit_time,
                        "deposit_end_time": proposal.get("deposit_end_time"),
                        "voting_start_time": proposal.get("voting_start_time"),
                        "voting_end_time": proposal.get("voting_end_time"),
                        "total_deposit": proposal.get("total_deposit", []),
                        "type": "governance_proposal",
                        "proposer": proposal.get("proposer", ""),
                        "final_tally": proposal.get("final_tally_result", {})
                    }
                    all_proposals.append(formatted)

        logger.info(f"Found {len(all_proposals)} proposals from past {days_back} days")
        return all_proposals

    async def get_proposal_votes(self, proposal_id: str) -> Dict[str, Any]:
        """Get voting details for a specific proposal"""
        data = await self._make_request(f"/cosmos/gov/v1/proposals/{proposal_id}/votes")
        if data and "votes" in data:
            return {
                "proposal_id": proposal_id,
                "total_votes": len(data["votes"]),
                "votes": data["votes"][:10]  # Sample of votes
            }
        return {}

    # ==================== ECOCREDIT MODULE ====================

    async def get_all_credit_classes(self) -> List[Dict[str, Any]]:
        """Get ALL credit classes"""
        logger.info("Fetching all credit classes...")

        data = await self._make_request(
            "/regen/ecocredit/v1/classes",
            params={"pagination.limit": "1000"}
        )

        classes = []
        if data and "classes" in data:
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

    async def get_all_credit_batches(self, days_back: int = 7) -> List[Dict[str, Any]]:
        """Get ALL credit batches from specified time period"""
        logger.info(f"Fetching all credit batches from past {days_back} days...")

        data = await self._make_request(
            "/regen/ecocredit/v1/batches",
            params={
                "pagination.limit": "1000",
                "pagination.reverse": "true"
            }
        )

        batches = []
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_back)

        if data and "batches" in data:
            for batch in data["batches"]:
                issuance_date = batch.get("issuance_date", "")
                if issuance_date:
                    try:
                        issuance_dt = datetime.fromisoformat(issuance_date.replace('Z', '+00:00'))
                        if issuance_dt >= cutoff_date:
                            formatted = {
                                "denom": batch.get("denom", ""),
                                "metadata": batch.get("metadata", ""),
                                "start_date": batch.get("start_date"),
                                "end_date": batch.get("end_date"),
                                "issuance_date": issuance_date,
                                "open": batch.get("open", False),
                                "type": "credit_batch"
                            }
                            batches.append(formatted)
                    except:
                        pass

        return batches

    async def get_batch_supply(self, batch_denom: str) -> Dict[str, Any]:
        """Get supply details for a credit batch"""
        data = await self._make_request(f"/regen/ecocredit/v1/batches/{batch_denom}/supply")
        return data if data else {}

    # ==================== MARKETPLACE MODULE ====================

    async def get_all_marketplace_activity(self, days_back: int = 7) -> Dict[str, Any]:
        """Get ALL marketplace activity"""
        logger.info("Fetching all marketplace activity...")

        # Get sell orders
        sell_orders_data = await self._make_request(
            "/regen/ecocredit/marketplace/v1/sell-orders",
            params={"pagination.limit": "1000"}
        )

        sell_orders = []
        if sell_orders_data and "sell_orders" in sell_orders_data:
            for order in sell_orders_data["sell_orders"]:
                formatted = {
                    "id": order.get("id"),
                    "seller": order.get("seller", ""),
                    "batch_denom": order.get("batch_denom", ""),
                    "quantity": order.get("quantity", ""),
                    "ask_price": order.get("ask_price", {}),
                    "disable_auto_retire": order.get("disable_auto_retire", False),
                    "expiration": order.get("expiration"),
                    "type": "sell_order"
                }
                sell_orders.append(formatted)

        # Get buy orders
        buy_orders_data = await self._make_request(
            "/regen/ecocredit/marketplace/v1/buy-orders",
            params={"pagination.limit": "1000"}
        )

        buy_orders = []
        if buy_orders_data and "buy_orders" in buy_orders_data:
            for order in buy_orders_data["buy_orders"]:
                formatted = {
                    "id": order.get("id"),
                    "buyer": order.get("buyer", ""),
                    "selection": order.get("selection", {}),
                    "quantity": order.get("quantity", ""),
                    "bid_price": order.get("bid_price", {}),
                    "disable_auto_retire": order.get("disable_auto_retire", False),
                    "disable_partial_fill": order.get("disable_partial_fill", False),
                    "expiration": order.get("expiration"),
                    "type": "buy_order"
                }
                buy_orders.append(formatted)

        return {
            "sell_orders": sell_orders,
            "buy_orders": buy_orders,
            "total_sell_orders": len(sell_orders),
            "total_buy_orders": len(buy_orders)
        }

    # ==================== BASKET MODULE ====================

    async def get_all_baskets(self) -> List[Dict[str, Any]]:
        """Get ALL basket tokens"""
        logger.info("Fetching all basket tokens...")

        data = await self._make_request(
            "/regen/ecocredit/basket/v1/baskets",
            params={"pagination.limit": "100"}
        )

        baskets = []
        if data and "baskets" in data:
            for basket in data["baskets"]:
                # Get basket balance details
                balance_data = await self._make_request(
                    f"/regen/ecocredit/basket/v1/baskets/{basket.get('basket_denom')}/balances"
                )

                formatted = {
                    "denom": basket.get("basket_denom", ""),
                    "name": basket.get("name", ""),
                    "credit_type": basket.get("credit_type_abbrev", ""),
                    "date_created": basket.get("date_created"),
                    "exponent": basket.get("exponent", 0),
                    "curator": basket.get("curator", ""),
                    "balances": balance_data.get("balances", []) if balance_data else [],
                    "balances_pagination": balance_data.get("pagination", {}) if balance_data else {},
                    "type": "basket"
                }
                baskets.append(formatted)

        return baskets

    # ==================== STAKING MODULE ====================

    async def get_staking_info(self) -> Dict[str, Any]:
        """Get comprehensive staking information"""
        logger.info("Fetching staking information...")

        # Get validators
        validators_data = await self._make_request(
            "/cosmos/staking/v1beta1/validators",
            params={"pagination.limit": "200", "status": "BOND_STATUS_BONDED"}
        )

        validators = []
        total_bonded = 0
        if validators_data and "validators" in validators_data:
            for val in validators_data["validators"]:
                validators.append({
                    "operator_address": val.get("operator_address"),
                    "moniker": val.get("description", {}).get("moniker", ""),
                    "tokens": val.get("tokens", "0"),
                    "delegator_shares": val.get("delegator_shares", "0"),
                    "commission_rate": val.get("commission", {}).get("commission_rates", {}).get("rate", "0"),
                    "status": val.get("status", "")
                })
                try:
                    total_bonded += int(val.get("tokens", "0"))
                except:
                    pass

        # Get staking parameters
        params_data = await self._make_request("/cosmos/staking/v1beta1/params")

        # Get staking pool
        pool_data = await self._make_request("/cosmos/staking/v1beta1/pool")

        return {
            "validators": validators[:10],  # Top 10 validators
            "total_validators": len(validators),
            "total_bonded_tokens": str(total_bonded),
            "params": params_data.get("params", {}) if params_data else {},
            "pool": pool_data.get("pool", {}) if pool_data else {}
        }

    # ==================== BANK MODULE ====================

    async def get_token_supply(self) -> Dict[str, Any]:
        """Get token supply information"""
        logger.info("Fetching token supply...")

        # Get total supply
        supply_data = await self._make_request("/cosmos/bank/v1beta1/supply")

        # Get REGEN specific supply
        regen_supply = await self._make_request("/cosmos/bank/v1beta1/supply/by_denom?denom=uregen")

        return {
            "total_supply": supply_data.get("supply", []) if supply_data else [],
            "regen_supply": regen_supply.get("amount", {}) if regen_supply else {},
            "pagination": supply_data.get("pagination", {}) if supply_data else {}
        }

    # ==================== RECENT BLOCKS & TRANSACTIONS ====================

    async def get_recent_blocks(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent blocks with transaction counts"""
        logger.info(f"Fetching {limit} recent blocks...")

        # Get latest block
        latest_block_data = await self._make_request("/cosmos/base/tendermint/v1beta1/blocks/latest")

        blocks = []
        if latest_block_data and "block" in latest_block_data:
            latest_height = int(latest_block_data["block"]["header"]["height"])

            # Get recent blocks
            for height in range(latest_height, max(1, latest_height - limit), -1):
                block_data = await self._make_request(f"/cosmos/base/tendermint/v1beta1/blocks/{height}")
                if block_data and "block" in block_data:
                    block = block_data["block"]
                    blocks.append({
                        "height": block["header"]["height"],
                        "time": block["header"]["time"],
                        "proposer": block["header"]["proposer_address"],
                        "tx_count": len(block.get("data", {}).get("txs", [])),
                        "hash": block_data.get("block_id", {}).get("hash", "")
                    })

        return blocks

    async def get_transaction_summary(self, days_back: int = 7) -> Dict[str, Any]:
        """Get transaction summary for the period"""
        logger.info(f"Fetching transaction summary for past {days_back} days...")

        # Unfortunately, Cosmos REST API doesn't provide an efficient way to get
        # total transaction counts for a time period without iterating through all blocks
        # or having an indexer. We have three options:
        # 1. Query every block (100,800 blocks/week = impractical)
        # 2. Use an external indexer service (requires additional infrastructure)
        # 3. Sample blocks and extrapolate (current approach)

        # For now, we'll acknowledge this limitation and not show misleading stats
        # In production, you'd want to use an indexer like:
        # - Mintscan API (requires API key)
        # - Custom indexer using Cosmos SDK events
        # - Graph Protocol subgraph

        tx_summary = {
            "note": "Transaction counts require indexer infrastructure",
            "total_transactions": None,  # Don't show misleading numbers
            "estimated": False,
            "by_type": {
                # These would need actual transaction querying to break down by type
                "bank_send": None,
                "staking_delegate": None,
                "staking_undelegate": None,
                "gov_vote": None,
                "gov_submit_proposal": None,
                "ecocredit_create_batch": None,
                "ecocredit_transfer": None,
                "marketplace_sell": None,
                "marketplace_buy": None,
                "ibc_transfer": None
            },
            "data_source": "Would require indexer or block iteration"
        }

        return tx_summary

    # ==================== IBC MODULE ====================

    async def get_ibc_activity(self) -> Dict[str, Any]:
        """Get IBC (Inter-Blockchain Communication) activity"""
        logger.info("Fetching IBC activity...")

        # Get IBC channels
        channels_data = await self._make_request(
            "/ibc/core/channel/v1/channels",
            params={"pagination.limit": "100"}
        )

        channels = []
        if channels_data and "channels" in channels_data:
            for channel in channels_data["channels"]:
                channels.append({
                    "channel_id": channel.get("channel_id"),
                    "port_id": channel.get("port_id"),
                    "state": channel.get("state"),
                    "counterparty": channel.get("counterparty", {}),
                    "connection_hops": channel.get("connection_hops", [])
                })

        # Get IBC connections
        connections_data = await self._make_request("/ibc/core/connection/v1/connections")

        return {
            "channels": channels,
            "total_channels": len(channels),
            "connections": connections_data.get("connections", [])[:10] if connections_data else [],
            "total_connections": len(connections_data.get("connections", [])) if connections_data else 0
        }

    # ==================== DATA MODULE (Regen-specific) ====================

    async def get_data_attestations(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent data attestations"""
        logger.info("Fetching data attestations...")

        # This would need the actual endpoint - placeholder for now
        # The regen data module stores attestations and anchors
        return []

    # ==================== COMPREHENSIVE SUMMARY ====================

    async def get_comprehensive_24h_summary(self) -> Dict[str, Any]:
        """
        Get a COMPLETE summary of ALL ledger activity in the past 24 hours
        """
        logger.info("Fetching comprehensive 24-hour ledger summary...")

        # Fetch all data types concurrently
        results = await asyncio.gather(
            self.get_all_proposals(days_back=1),
            self.get_all_credit_batches(days_back=1),
            self.get_all_marketplace_activity(days_back=1),
            self.get_all_baskets(),
            self.get_staking_info(),
            self.get_token_supply(),
            self.get_recent_blocks(limit=20),
            self.get_ibc_activity(),
            return_exceptions=True
        )

        summary = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "period": "24_hours",
            "proposals": results[0] if not isinstance(results[0], Exception) else [],
            "credit_batches": results[1] if not isinstance(results[1], Exception) else [],
            "marketplace": results[2] if not isinstance(results[2], Exception) else {},
            "baskets": results[3] if not isinstance(results[3], Exception) else [],
            "staking": results[4] if not isinstance(results[4], Exception) else {},
            "token_supply": results[5] if not isinstance(results[5], Exception) else {},
            "recent_blocks": results[6] if not isinstance(results[6], Exception) else [],
            "ibc": results[7] if not isinstance(results[7], Exception) else {}
        }

        # Calculate comprehensive statistics (with safe access)
        proposals = summary.get("proposals", [])
        credit_batches = summary.get("credit_batches", [])
        baskets = summary.get("baskets", [])
        marketplace = summary.get("marketplace", {})
        staking = summary.get("staking", {})
        recent_blocks = summary.get("recent_blocks", [])
        ibc = summary.get("ibc", {})

        stats = {
            "governance": {
                "total_proposals": len(proposals),
                "active_proposals": len([p for p in proposals if p.get("status") == "VOTING_PERIOD"]),
                "passed_24h": len([p for p in proposals if p.get("status") == "PASSED"])
            },
            "ecocredit": {
                "new_batches": len(credit_batches),
                "total_baskets": len(baskets)
            },
            "marketplace": {
                "active_sell_orders": marketplace.get("total_sell_orders", 0),
                "active_buy_orders": marketplace.get("total_buy_orders", 0)
            },
            "staking": {
                "total_validators": staking.get("total_validators", 0),
                "total_bonded": staking.get("total_bonded_tokens", "0")
            },
            "network": {
                "blocks_24h": len(recent_blocks),
                "avg_tx_per_block": sum(b.get("tx_count", 0) for b in recent_blocks) / max(len(recent_blocks), 1),
                "ibc_channels": ibc.get("total_channels", 0)
            }
        }
        summary["statistics"] = stats

        logger.info(f"Comprehensive 24h summary: {stats}")
        return summary

    async def get_comprehensive_weekly_summary(self) -> Dict[str, Any]:
        """
        Get a COMPLETE summary of ALL ledger activity in the past 7 days
        """
        logger.info("Fetching comprehensive weekly ledger summary...")

        # Fetch all data types concurrently
        results = await asyncio.gather(
            self.get_all_proposals(days_back=7),
            self.get_all_credit_batches(days_back=7),
            self.get_all_marketplace_activity(days_back=7),
            self.get_all_baskets(),
            self.get_staking_info(),
            self.get_token_supply(),
            self.get_recent_blocks(limit=100),
            self.get_ibc_activity(),
            self.get_transaction_summary(days_back=7),
            return_exceptions=True
        )

        summary = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "period": "7_days",
            "proposals": results[0] if not isinstance(results[0], Exception) else [],
            "credit_batches": results[1] if not isinstance(results[1], Exception) else [],
            "marketplace": results[2] if not isinstance(results[2], Exception) else {},
            "baskets": results[3] if not isinstance(results[3], Exception) else [],
            "staking": results[4] if not isinstance(results[4], Exception) else {},
            "token_supply": results[5] if not isinstance(results[5], Exception) else {},
            "recent_blocks": results[6] if not isinstance(results[6], Exception) else [],
            "ibc": results[7] if not isinstance(results[7], Exception) else {},
            "transactions": results[8] if not isinstance(results[8], Exception) else {}
        }

        # Calculate comprehensive weekly statistics (with safe access)
        proposals = summary.get("proposals", [])
        credit_batches = summary.get("credit_batches", [])
        baskets = summary.get("baskets", [])
        marketplace = summary.get("marketplace", {})
        staking = summary.get("staking", {})

        stats = {
            "governance": {
                "total_proposals": len(proposals),
                "active_proposals": len([p for p in proposals if p.get("status") == "VOTING_PERIOD"]),
                "passed_week": len([p for p in proposals if p.get("status") == "PASSED"]),
                "rejected_week": len([p for p in proposals if p.get("status") == "REJECTED"])
            },
            "ecocredit": {
                "new_batches": len(credit_batches),
                "total_baskets": len(baskets),
                "basket_names": [b.get("name", "") for b in baskets]
            },
            "marketplace": {
                "active_sell_orders": marketplace.get("total_sell_orders", 0),
                "active_buy_orders": marketplace.get("total_buy_orders", 0),
                "total_listings": marketplace.get("total_sell_orders", 0) + marketplace.get("total_buy_orders", 0)
            },
            "staking": {
                "total_validators": staking.get("total_validators", 0),
                "total_bonded": staking.get("total_bonded_tokens", "0"),
                "top_validators": [v.get("moniker", "") for v in staking.get("validators", [])[:5]]
            },
            "network": {
                "blocks_processed": len(summary.get("recent_blocks", [])),
                "total_transactions": summary.get("transaction_summary", {}).get("total_transactions"),
                "avg_tx_per_block": None,  # Can't calculate without proper tx counts
                "ibc_channels": summary.get("ibc", {}).get("total_channels", 0),
                "ibc_connections": summary.get("ibc", {}).get("total_connections", 0),
                "tx_data_note": summary.get("transaction_summary", {}).get("note", "")
            }
        }
        summary["statistics"] = stats

        logger.info(f"Comprehensive weekly summary: {stats}")
        return summary

    def format_comprehensive_daily_post(self, summary: Dict[str, Any]) -> str:
        """
        Format comprehensive ledger data for daily social media posts
        """
        stats = summary.get("statistics", {})
        lines = ["📊 **Regen Network 24h Update**\n"]

        # Governance
        gov_stats = stats.get("governance", {})
        if gov_stats.get("active_proposals", 0) > 0:
            lines.append(f"🗳️ {gov_stats['active_proposals']} active proposal(s)")
            for prop in summary.get("proposals", [])[:2]:
                if prop.get("status") == "VOTING_PERIOD":
                    lines.append(f"  • #{prop['id']}: {prop['title'][:40]}...")

        # Credits and marketplace
        eco_stats = stats.get("ecocredit", {})
        market_stats = stats.get("marketplace", {})
        if eco_stats.get("new_batches", 0) > 0:
            lines.append(f"🌱 {eco_stats['new_batches']} new credit batches issued")
        if market_stats.get("total_listings", 0) > 0:
            lines.append(f"💱 {market_stats['active_sell_orders']} sell / {market_stats['active_buy_orders']} buy orders")

        # Network activity
        net_stats = stats.get("network", {})
        lines.append(f"⛓️ Network: {net_stats.get('avg_tx_per_block', 0):.1f} tx/block avg")

        # Staking
        staking_stats = stats.get("staking", {})
        lines.append(f"🔒 {staking_stats.get('total_validators', 0)} active validators")

        # IBC
        if net_stats.get("ibc_channels", 0) > 0:
            lines.append(f"🌉 {net_stats['ibc_channels']} IBC channels active")

        return "\n".join(lines)

    def format_comprehensive_weekly_post(self, summary: Dict[str, Any]) -> str:
        """
        Format comprehensive ledger data for WEEKLY social media posts
        """
        stats = summary.get("statistics", {})
        lines = ["📊 **Regen Network 7-Day Update**\n"]

        # Governance - Show actual proposals
        gov_stats = stats.get("governance", {})
        proposals = summary.get("proposals", [])
        if gov_stats.get("total_proposals", 0) > 0 or len(proposals) > 0:
            lines.append(f"\n**🗳️ Governance Activity**")
            if gov_stats.get("active_proposals", 0) > 0:
                lines.append(f"• {gov_stats['active_proposals']} active proposal(s)")
                # Show active proposals
                for prop in proposals:
                    if prop.get("status") == "VOTING_PERIOD":
                        lines.append(f"  - #{prop['id']}: {prop['title'][:50]}...")
                        if prop.get("voting_end_time"):
                            end_time = prop['voting_end_time'].split('T')[0]
                            lines.append(f"    Voting ends: {end_time}")
            if gov_stats.get("passed_week", 0) > 0:
                lines.append(f"• ✅ {gov_stats['passed_week']} proposal(s) passed")
                # Show passed proposals
                for prop in proposals[:2]:  # Limit to 2
                    if prop.get("status") == "PASSED":
                        lines.append(f"  - #{prop['id']}: {prop['title'][:50]}")
            if gov_stats.get("rejected_week", 0) > 0:
                lines.append(f"• ❌ {gov_stats['rejected_week']} proposal(s) rejected")

        # Credits and marketplace
        lines.append(f"\n**💱 Market & Credits**")
        eco_stats = stats.get("ecocredit", {})
        market_stats = stats.get("marketplace", {})
        if eco_stats.get("new_batches", 0) > 0:
            lines.append(f"• 🌱 {eco_stats['new_batches']} new credit batches issued")
        lines.append(f"• 📈 {market_stats.get('active_sell_orders', 0)} sell / {market_stats.get('active_buy_orders', 0)} buy orders")

        # Network activity
        lines.append(f"\n**⛓️ Network Stats**")
        net_stats = stats.get("network", {})
        # Don't show transaction counts if we don't have accurate data
        # Only show metrics we can accurately measure

        # Staking
        staking_stats = stats.get("staking", {})
        lines.append(f"• 🔒 {staking_stats.get('total_validators', 0)} active validators")

        # Bonded tokens
        bonded = staking_stats.get("total_bonded", "0")
        if bonded and bonded != "0":
            try:
                bonded_millions = int(bonded) / 10**6 / 1000000 if isinstance(bonded, str) else bonded / 10**6
                lines.append(f"• 💰 {bonded_millions:.1f}M REGEN bonded")
            except (ValueError, TypeError):
                pass  # Skip if can't convert bonded amount

        # IBC
        lines.append(f"• 🌉 {net_stats.get('ibc_channels', 0)} IBC channels")

        return "\n".join(lines)

    def format_comprehensive_weekly_digest(self, summary: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format comprehensive ledger data for weekly digest
        """
        # Extract nested stats from the comprehensive summary statistics
        stats = summary.get("statistics", {})
        gov_stats = stats.get("governance", {})
        eco_stats = stats.get("ecocredit", {})
        market_stats = stats.get("marketplace", {})
        staking_stats = stats.get("staking", {})
        network_stats = stats.get("network", {})

        # Flatten stats into format expected by weekly curator
        flattened_stats = {
            "total_credit_batches": eco_stats.get("new_batches", 0),
            "new_credits": eco_stats.get("new_batches", 0),  # Same as total for weekly
            "active_proposals": gov_stats.get("active_proposals", 0),
            "completed_proposals": gov_stats.get("passed_week", 0) + gov_stats.get("rejected_week", 0),
            "sell_orders": market_stats.get("active_sell_orders", 0),
            "buy_orders": market_stats.get("active_buy_orders", 0),
            "total_validators": staking_stats.get("total_validators", 0),
            "ibc_channels": network_stats.get("ibc_channels", 0),
            "total_transactions": network_stats.get("total_transactions", 0)
        }

        digest_data = {
            "title": "Regen Network Weekly On-Chain Activity",
            "summary": self.format_comprehensive_weekly_post(summary),
            "statistics": flattened_stats,  # Add flattened statistics for weekly curator
            "sections": [],
            "proposals": summary.get("proposals", []),  # Include proposals list
            "credit_batches": summary.get("credit_batches", [])  # Include batches list
        }

        # Governance section with details
        if summary.get("proposals"):
            gov_section = {
                "title": "📊 Governance Activity",
                "items": [],
                "stats": gov_stats
            }
            for prop in summary.get("proposals", [])[:10]:
                gov_section["items"].append({
                    "title": f"Proposal #{prop['id']}: {prop['title']}",
                    "description": prop.get("summary", "")[:200],
                    "status": prop.get("status", ""),
                    "voting_end": prop.get("voting_end_time", ""),
                    "link": f"https://www.mintscan.io/regen/proposals/{prop['id']}"
                })
            digest_data["sections"].append(gov_section)

        # Credit issuance section with details
        if summary.get("credit_batches"):
            credit_section = {
                "title": "🌱 Ecocredit Activity",
                "items": [],
                "stats": eco_stats
            }
            for batch in summary.get("credit_batches", [])[:10]:
                credit_section["items"].append({
                    "title": f"Batch: {batch['denom']}",
                    "issuance_date": batch.get("issuance_date", ""),
                    "period": f"{batch.get('start_date', '')} to {batch.get('end_date', '')}",
                    "metadata": batch.get("metadata", "")
                })
            digest_data["sections"].append(credit_section)

        # Marketplace section with volume
        market = summary.get("marketplace", {})
        if market.get("sell_orders") or market.get("buy_orders"):
            market_section = {
                "title": "💱 Marketplace Activity",
                "stats": market_stats,
                "items": []
            }

            # Add sell orders
            for order in market.get("sell_orders", [])[:5]:
                market_section["items"].append({
                    "type": "sell",
                    "id": order['id'],
                    "batch": order["batch_denom"],
                    "quantity": order["quantity"],
                    "price": f"{order.get('ask_price', {}).get('amount', '')} {order.get('ask_price', {}).get('denom', '')}"
                })

            # Add buy orders
            for order in market.get("buy_orders", [])[:5]:
                market_section["items"].append({
                    "type": "buy",
                    "id": order['id'],
                    "quantity": order["quantity"],
                    "price": f"{order.get('bid_price', {}).get('amount', '')} {order.get('bid_price', {}).get('denom', '')}"
                })

            digest_data["sections"].append(market_section)

        # Network statistics section
        avg_tx = network_stats.get('avg_tx_per_block')
        avg_tx_str = f"{avg_tx:.2f}" if avg_tx is not None else "N/A"
        total_tx = network_stats.get("total_transactions")
        total_tx_str = str(total_tx) if total_tx is not None else "N/A"

        network_section = {
            "title": "⛓️ Network Statistics",
            "stats": network_stats,
            "items": [
                {"metric": "Total Transactions", "value": total_tx_str},
                {"metric": "Avg TX/Block", "value": avg_tx_str},
                {"metric": "Active Validators", "value": staking_stats.get("total_validators", 0)},
                {"metric": "IBC Channels", "value": network_stats.get("ibc_channels", 0)}
            ]
        }
        digest_data["sections"].append(network_section)

        # Top validators
        if summary.get("staking", {}).get("validators"):
            validator_section = {
                "title": "🔒 Top Validators",
                "items": []
            }
            for val in summary.get("staking", {}).get("validators", [])[:5]:
                validator_section["items"].append({
                    "name": val.get("moniker", "Unknown"),
                    "tokens": val.get("tokens", "0"),
                    "commission": val.get("commission_rate", "0")
                })
            digest_data["sections"].append(validator_section)

        return digest_data


# Test function
async def test_comprehensive_client():
    """Test the comprehensive Regen ledger client"""
    client = RegenLedgerComprehensive()

    print("Testing Comprehensive Regen Ledger Client...")
    print("=" * 60)

    # Test 24h summary
    print("\nFetching 24-hour comprehensive summary...")
    daily_summary = await client.get_comprehensive_24h_summary()

    print("\n24-Hour Statistics:")
    stats = daily_summary.get("statistics", {})
    for category, data in stats.items():
        print(f"\n{category.upper()}:")
        for key, value in data.items():
            print(f"  {key}: {value}")

    print("\n" + "-" * 50)

    # Test formatted daily post
    print("\nFormatted Daily Post:")
    daily_post = client.format_comprehensive_daily_post(daily_summary)
    print(daily_post)

    print("\n" + "=" * 60)
    print("✅ Comprehensive test complete!")


if __name__ == "__main__":
    asyncio.run(test_comprehensive_client())