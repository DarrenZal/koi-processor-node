from __future__ import annotations

import logging

from api import crawl_llm

logger = logging.getLogger(__name__)


async def parse_relate_clause(instruction: str) -> dict:
    targets, usage = await crawl_llm.parse_relate_clause(instruction=instruction)
    usd = crawl_llm.estimate_usd(usage, crawl_llm.DEFAULT_MODEL)
    logger.info(
        'parse_relate_clause telemetry: {"surface":"parse_relate_clause","tokens_in":%d,"tokens_out":%d,"usd":%.6f}',
        usage.get("prompt_tokens", 0),
        usage.get("completion_tokens", 0),
        usd,
    )
    return {"targets": targets, "usage": usage, "usd": usd}
