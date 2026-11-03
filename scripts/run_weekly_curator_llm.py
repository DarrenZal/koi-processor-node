#!/usr/bin/env python3
"""
CLI Runner for Weekly Curator with LLM
Uses the new LLM-powered weekly curator that includes ALL content

With --apply it runs src/content/weekly_curator_llm.py's main(): one paid chat
completion, an INSERT into quality_reviews and file exports. With no arguments or
--dry-run it only prints what it would do.
"""

import sys
import os
import asyncio
import logging
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main(start_date=None, end_date=None):
    """Main execution function

    Args:
        start_date: Start date for digest (YYYY-MM-DD) or None for 7 days ago
        end_date: End date for digest (YYYY-MM-DD) or None for today
    """
    try:
        # Import the new curator's main function directly
        # This avoids duplication since the main() already saves to DB
        from src.content.weekly_curator_llm import main as curator_main

        # Set environment variables for date range if provided
        if start_date:
            os.environ['DIGEST_START_DATE'] = start_date
        if end_date:
            os.environ['DIGEST_END_DATE'] = end_date

        logger.info(f"Generating weekly digest with LLM (includes ALL content)... Date range: {start_date or '7 days ago'} to {end_date or 'today'}")

        # Run the curator's main function which handles everything
        digest = await curator_main()

        if digest:
            # Return success (digest already saved to DB and exported)
            return 0
        else:
            print("❌ Failed to generate weekly digest")
            return 1

    except Exception as e:
        logger.error(f"Error generating weekly digest: {e}")
        print(f"❌ Error: {e}")
        return 1

def cli_main(argv=None) -> int:
    """Command-line entry: a dry run unless --apply is given."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Generate weekly digest with optional date range. '
                                                 'Default is a dry run that contacts nothing.')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD), defaults to 7 days ago')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD), defaults to today')
    parser.add_argument('--dry-run', action='store_true',
                        help='Report what would happen and exit (the default; wins over --apply)')
    parser.add_argument('--apply', action='store_true',
                        help='Run it: one paid chat completion, a database INSERT and file exports')
    args = parser.parse_args(argv)

    if args.dry_run or not args.apply:
        try:
            from src.content.weekly_curator_llm import describe_plan
            plan = describe_plan()
        except Exception as e:  # missing optional packages should not break a dry run
            plan = [f"(could not load src/content/weekly_curator_llm.py: {e})",
                    "Steps: one paid chat completion, an INSERT into quality_reviews and file exports"]
        for line in plan:
            if line.startswith("Date range:"):
                line = f"Date range: {args.start_date or '7 days ago'} to {args.end_date or 'today'}"
            print(line)
        print("\nDry run: nothing was read, sent or written. Re-run with --apply to do this.")
        return 0

    # Run async main
    return asyncio.run(main(args.start_date, args.end_date))


if __name__ == "__main__":
    sys.exit(cli_main())