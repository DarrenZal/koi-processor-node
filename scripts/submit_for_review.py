#!/usr/bin/env python3
"""
Submit content for quality control review
Used by cron jobs to submit generated content for review before publishing
"""

import asyncio
import sys
import json
from pathlib import Path
from datetime import datetime, timezone
import argparse
from loguru import logger

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.content.quality_control import QualityControl, ContentType, ApprovalStatus


async def submit_content(content_path: str, content_type: ContentType, auto_review: bool = False):
    """
    Submit content for review

    Args:
        content_path: Path to content JSON file
        content_type: Type of content (daily_thread or weekly_digest)
        auto_review: If True, performs automatic quality checks and auto-approves if passing
    """
    # Load content
    content_file = Path(content_path)
    if not content_file.exists():
        logger.error(f"Content file not found: {content_path}")
        return False

    with open(content_file, 'r') as f:
        content = json.load(f)

    # Initialize quality control
    qc = QualityControl()
    await qc.initialize_db()

    try:
        # Perform quality validation
        validation_results = await qc.validate_content(content, content_type)
        quality_score = validation_results['overall_score']

        logger.info(f"Content quality score: {quality_score:.2f}")
        logger.info(f"Validation results: {validation_results}")

        # Check if auto-publish is enabled and content meets threshold
        if auto_review and qc.auto_publish_enabled:
            if quality_score >= qc.auto_publish_config.get('quality_threshold', 0.85):
                # Auto-approve high-quality content after week 1
                status = ApprovalStatus.APPROVED
                logger.info("✅ Content auto-approved due to high quality score")
            else:
                status = ApprovalStatus.PENDING_REVIEW
                logger.info("⏳ Content requires manual review")
        else:
            # Always require manual review in first week
            status = ApprovalStatus.PENDING_REVIEW
            logger.info("⏳ Content submitted for manual review")

        # Store in database for review
        review_id = await qc.store_for_review(
            content=content,
            content_type=content_type,
            validation_results=validation_results,
            status=status
        )

        logger.info(f"Content submitted with review ID: {review_id}")

        # If auto-approved, mark for publishing
        if status == ApprovalStatus.APPROVED:
            await qc.mark_for_publishing(review_id)
            logger.info(f"Content marked for auto-publishing")

        return True

    except Exception as e:
        logger.error(f"Error submitting content for review: {e}")
        return False

    finally:
        await qc.cleanup()


async def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Submit content for quality review')
    parser.add_argument('content_file', help='Path to content JSON file')
    parser.add_argument('--type', choices=['daily', 'weekly'], required=True,
                       help='Type of content')
    parser.add_argument('--auto-review', action='store_true',
                       help='Enable automatic review and approval if quality threshold met')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose logging')

    args = parser.parse_args()

    # Setup logging
    level = "DEBUG" if args.verbose else "INFO"
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level=level
    )

    # Map content type
    content_type = ContentType.DAILY_THREAD if args.type == 'daily' else ContentType.WEEKLY_DIGEST

    # Submit content
    success = await submit_content(
        content_path=args.content_file,
        content_type=content_type,
        auto_review=args.auto_review
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(main())