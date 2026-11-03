#!/usr/bin/env python3
"""
CLI Runner for Daily Content Curator
Command-line interface for generating daily threads and weekly digests
"""

import asyncio
import argparse
import json
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from loguru import logger
import asyncpg

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Try to use LLM version if available, fallback to basic version
try:
    from src.content.daily_curator_llm import DailyCuratorLLM as DailyCurator
    print("Using LLM-enhanced curator")
except ImportError:
    from src.content.daily_curator import DailyCurator
    print("Using basic curator (LLM not available)")


def setup_logging(verbose: bool = False):
    """Configure logging"""
    level = "DEBUG" if verbose else "INFO"
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>",
        level=level
    )
    
    # Also log to file
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    logger.add(
        log_dir / "daily_curator.log",
        rotation="10 MB",
        retention="7 days",
        level="DEBUG"
    )


async def generate_daily_thread(curator: DailyCurator, output_path: Optional[str] = None):
    """Generate and save daily thread"""
    logger.info("Generating daily thread...")
    
    try:
        thread = await curator.generate_daily_thread()
        
        # Display thread
        print("\n" + "="*60)
        print("DAILY THREAD - " + datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        print("="*60)
        
        for i, post in enumerate(thread['posts'], 1):
            print(f"\n--- Post {i} ({post['type']}) ---")
            print(post['content'])
            if post.get('url'):
                print(f"Link: {post['url']}")
            if post.get('published_at'):
                print(f"Published: {post['published_at']}")

            # Display sources if available
            if post.get('sources'):
                print("\nSources:")
                for source in post['sources']:
                    if isinstance(source, dict):
                        if source.get('type') == 'aggregated':
                            print(f"  - {source.get('description', 'Aggregated content')}")
                            if source.get('sensor_breakdown'):
                                for sensor, count in source['sensor_breakdown'].items():
                                    print(f"    • {sensor}: {count} items")
                        elif source.get('type') == 'fallback':
                            print(f"  - {source.get('description', 'Fallback content')}")
                        elif source.get('type') == 'ledger':
                            print(f"  - {source.get('description', 'Ledger data')}")
                        else:
                            sensor = source.get('sensor', 'unknown')
                            event = source.get('event_type', 'unknown')
                            date = source.get('published_at', 'unknown')
                            url = source.get('url', '')
                            print(f"  - {sensor} ({event}) - {date}")
                            if url:
                                print(f"    URL: {url}")
        
        # Display metadata
        print("\n--- Metadata ---")
        metadata = thread.get('metadata', {})
        print(f"New content today: {metadata.get('content_sources', {}).get('new_today', 0)}")
        print(f"Recent content (48h): {metadata.get('content_sources', {}).get('recent_48h', 0)}")
        print(f"Trending topics: {metadata.get('content_sources', {}).get('trending_topics', 0)}")
        
        # Save to file if requested
        if output_path:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w') as f:
                json.dump(thread, f, indent=2, default=str)
            
            logger.info(f"Thread saved to {output_file}")
        
        return thread
        
    except Exception as e:
        logger.error(f"Failed to generate daily thread: {e}")
        raise


async def generate_weekly_digest(curator: DailyCurator, output_path: Optional[str] = None):
    """Generate and save weekly digest"""
    logger.info("Generating weekly digest...")
    
    try:
        digest = await curator.generate_weekly_digest()
        
        # Display digest
        print("\n" + "="*60)
        print("WEEKLY DIGEST - Week ending " + datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        print("="*60)
        
        for section in digest.get('sections', []):
            print(f"\n## {section['title']}")
            print("-" * 40)
            print(section['content'])
        
        # Display metadata
        print("\n--- Metadata ---")
        metadata = digest.get('metadata', {})
        print(f"Total content items: {metadata.get('total_content', 0)}")
        print(f"Trending topics: {metadata.get('trending_topics', 0)}")
        
        # Export to NotebookLM format
        markdown = await curator.export_for_notebooklm(digest)
        
        # Save to file if requested
        if output_path:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Save JSON version
            json_file = output_file.with_suffix('.json')
            with open(json_file, 'w') as f:
                json.dump(digest, f, indent=2, default=str)
            
            # Save Markdown version
            md_file = output_file.with_suffix('.md')
            with open(md_file, 'w') as f:
                f.write(markdown)
            
            logger.info(f"Digest saved to {json_file} and {md_file}")
        
        return digest
        
    except Exception as e:
        logger.error(f"Failed to generate weekly digest: {e}")
        raise


async def check_content_status(curator: DailyCurator):
    """Check status of available content"""
    logger.info("Checking content status...")
    
    try:
        # Get content from different time windows
        new_24h = await curator.get_recent_published_content(hours=24, min_confidence=0.7)
        recent_48h = await curator.get_recent_published_content(hours=48, min_confidence=0.6)
        week_content = await curator.get_recent_published_content(hours=168, min_confidence=0.5)
        
        # Get ledger stats
        stats = await curator.get_ledger_stats(hours=24)
        
        print("\n" + "="*60)
        print("CONTENT STATUS REPORT")
        print("="*60)
        
        print(f"\nContent published in last 24 hours: {len(new_24h)}")
        if new_24h:
            print("  Sources:", {item['source_sensor'] for item in new_24h})
        
        print(f"\nContent published in last 48 hours: {len(recent_48h)}")
        if recent_48h:
            print("  Sources:", {item['source_sensor'] for item in recent_48h})
        
        print(f"\nContent published in last week: {len(week_content)}")
        if week_content:
            source_counts = {}
            for item in week_content:
                source = item['source_sensor']
                source_counts[source] = source_counts.get(source, 0) + 1
            print("  Source breakdown:")
            for source, count in sorted(source_counts.items(), key=lambda x: x[1], reverse=True):
                print(f"    - {source}: {count}")
        
        print("\nLedger Statistics (24h):")
        for key, value in stats.items():
            print(f"  - {key}: {value}")
        
        # Check publication date confidence
        if week_content:
            confidence_levels = [item.get('published_confidence', 0) for item in week_content]
            avg_confidence = sum(confidence_levels) / len(confidence_levels) if confidence_levels else 0
            print(f"\nAverage publication date confidence: {avg_confidence:.2%}")
            
            high_confidence = len([c for c in confidence_levels if c >= 0.8])
            medium_confidence = len([c for c in confidence_levels if 0.5 <= c < 0.8])
            low_confidence = len([c for c in confidence_levels if c < 0.5])
            
            print(f"  High confidence (≥0.8): {high_confidence}")
            print(f"  Medium confidence (0.5-0.8): {medium_confidence}")
            print(f"  Low confidence (<0.5): {low_confidence}")
        
    except Exception as e:
        logger.error(f"Failed to check content status: {e}")
        raise


async def run_migration(curator: DailyCurator):
    """Run database migration for publication date fields"""
    logger.info("Running database migration...")
    
    try:
        migration_file = Path(__file__).parent.parent / "migrations" / "004_add_publication_dates.sql"
        
        if not migration_file.exists():
            logger.error(f"Migration file not found: {migration_file}")
            return
        
        with open(migration_file, 'r') as f:
            migration_sql = f.read()
        
        async with asyncpg.create_pool(curator.db_url) as pool:
            async with pool.acquire() as conn:
                # Execute migration
                await conn.execute(migration_sql)
                logger.info("Migration completed successfully")
                
                # Verify new columns exist
                result = await conn.fetch("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = 'koi_memories' 
                    AND column_name IN ('published_at', 'published_confidence', 'content_hash')
                """)
                
                if result:
                    print("\nNew columns added:")
                    for row in result:
                        print(f"  - {row['column_name']}: {row['data_type']}")
                else:
                    logger.warning("New columns not found - migration may have failed")
                    
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Daily Content Curator CLI - Generate threads and digests from KOI infrastructure"
    )
    
    parser.add_argument(
        'command',
        choices=['daily', 'weekly', 'status', 'migrate'],
        help='Command to run'
    )
    
    parser.add_argument(
        '--config',
        default='config/curator_config.yaml',
        help='Path to configuration file'
    )
    
    parser.add_argument(
        '--output',
        '-o',
        help='Output file path for generated content'
    )
    
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.verbose)
    
    # Create curator instance
    curator = DailyCurator(config_path=args.config)
    
    # Run selected command
    if args.command == 'daily':
        asyncio.run(generate_daily_thread(curator, args.output))
    elif args.command == 'weekly':
        asyncio.run(generate_weekly_digest(curator, args.output))
    elif args.command == 'status':
        asyncio.run(check_content_status(curator))
    elif args.command == 'migrate':
        asyncio.run(run_migration(curator))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()