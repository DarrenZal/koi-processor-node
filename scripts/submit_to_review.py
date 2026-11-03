#!/usr/bin/env python3
"""
Submit generated content to quality_reviews table for dashboard review
"""

import json
import psycopg2
import uuid
import sys
from pathlib import Path
from datetime import datetime, timezone
import os
from dotenv import load_dotenv

load_dotenv()

def submit_to_review(file_path: str, content_type: str):
    """Submit a generated content file to the quality_reviews table"""

    # Load the generated content
    with open(file_path, 'r') as f:
        content = json.load(f)

    # Extract comprehensive provenance information
    provenance = {
        "sources": [],
        "platforms": [],
        "source_count": 0,
        "generation_metadata": {},
        "cat_receipts": [],  # Content Authentication Technology receipts
        "transformation_pipeline": [],  # How content was transformed
        "source_memories": []  # Original KOI memories used
    }

    # Extract source information from content metadata
    if 'metadata' in content:
        metadata = content['metadata']

        # Extract content sources info
        if 'content_sources' in metadata:
            content_sources = metadata['content_sources']
            # Count total content items
            provenance['source_count'] = content_sources.get('total_content_24h', 0)

            # Add generation metadata
            provenance['generation_metadata'] = {
                'themes': content_sources.get('themes', []),
                'trending': content_sources.get('trending', []),
                'total_content_24h': content_sources.get('total_content_24h', 0),
                'new_today': content_sources.get('new_today', 0),
                'recent_48h': content_sources.get('recent_48h', 0)
            }

        # Extract source memory references (rid/cid from KOI)
        if 'source_memories' in metadata:
            for memory in metadata['source_memories']:
                provenance['source_memories'].append({
                    'rid': memory.get('rid'),  # Receipt ID
                    'cid': memory.get('cid'),  # Content ID
                    'source_sensor': memory.get('source_sensor'),
                    'event_type': memory.get('event_type'),
                    'ingested_at': str(memory.get('ingested_at', ''))
                })

                # Extract unique source sensors as platforms
                if memory.get('source_sensor'):
                    if memory['source_sensor'] not in provenance['platforms']:
                        provenance['platforms'].append(memory['source_sensor'])

        # Extract REAL detailed sources from database
        detailed_sources = []

        try:
            # Connect to get real source data
            source_conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST', 'localhost'),
                port=os.getenv('POSTGRES_PORT', 5433),
                database=os.getenv('POSTGRES_DB', 'eliza'),
                user=os.getenv('POSTGRES_USER', 'postgres'),
                password=os.getenv('POSTGRES_PASSWORD', 'postgres')
            )
            source_cur = source_conn.cursor()

            # Try koi_content first, but it may be empty
            source_cur.execute("""
                SELECT rid, title, url, metadata, created_at
                FROM koi_content
                WHERE created_at > NOW() - INTERVAL '3 days'
                AND metadata IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 30
            """)

            koi_contents = source_cur.fetchall()

            for content_row in koi_contents:
                rid, title, url, meta, created_at = content_row

                if meta:
                    # Determine platform from metadata or rid
                    platform = 'unknown'
                    if 'forum' in str(meta).lower() or 'discourse' in str(rid).lower():
                        platform = 'forum'
                    elif 'telegram' in str(meta).lower():
                        platform = 'telegram'
                    elif 'notion' in str(meta).lower():
                        platform = 'notion'
                    elif 'twitter' in str(meta).lower():
                        platform = 'twitter'
                    elif 'discord' in str(meta).lower():
                        platform = 'discord'
                    elif 'website' in str(meta).lower() or 'web' in str(rid).lower():
                        platform = 'website'

                    source_item = {
                        'platform': platform,
                        'type': meta.get('type', meta.get('source_type', 'content')),
                        'title': title or meta.get('topic_title', meta.get('title', 'Untitled'))[:100]
                    }

                    # Add URL - prioritize post_url from improved sensors
                    if 'post_url' in meta:
                        source_item['url'] = meta['post_url']
                    elif url:
                        source_item['url'] = url
                    elif 'page_url' in meta:
                        source_item['url'] = meta['page_url']

                    # Add timestamp
                    if 'created_at' in meta:
                        source_item['timestamp'] = meta['created_at']
                    elif 'published_at' in meta:
                        source_item['timestamp'] = meta['published_at']
                    elif created_at:
                        source_item['timestamp'] = created_at.isoformat()

                    # Add author - from improved sensor metadata
                    if 'author' in meta:
                        source_item['author'] = meta['author']
                    elif 'username' in meta:
                        source_item['author'] = meta['username']

                    # Add post-specific data from improved sensors
                    if 'post_number' in meta:
                        source_item['post_number'] = meta['post_number']
                        source_item['type'] = 'post'  # Override type for forum posts
                    if 'post_id' in meta:
                        source_item['post_id'] = meta['post_id']
                    if 'channel' in meta:
                        source_item['channel'] = meta['channel']
                    if 'reply_to_post_number' in meta:
                        source_item['reply_to'] = f"Post #{meta['reply_to_post_number']}"

                    # Extract content excerpt if available
                    if 'content' in meta and meta['content']:
                        source_item['excerpt'] = str(meta['content'])[:200] + '...'
                    elif 'excerpt' in meta:
                        source_item['excerpt'] = meta['excerpt']

                    detailed_sources.append(source_item)

            # Query memories table for KOI documents (where sensor data is actually stored)
            source_cur.execute("""
                SELECT content, metadata, "createdAt"
                FROM memories
                WHERE "createdAt" > NOW() - INTERVAL '3 days'
                AND type = 'koi_document'
                AND content IS NOT NULL
                ORDER BY "createdAt" DESC
                LIMIT 50
            """)

            memories = source_cur.fetchall()

            for memory in memories:
                content_data, meta, created_at = memory

                if content_data and isinstance(content_data, dict):
                    # Determine platform from content data
                    platform = 'unknown'
                    if 'source' in content_data:
                        source = content_data['source']
                        if 'discourse' in source:
                            platform = 'forum'
                        elif 'telegram' in source:
                            platform = 'telegram'
                        elif 'notion' in source:
                            platform = 'notion'
                        elif 'twitter' in source:
                            platform = 'twitter'
                        elif 'discord' in source:
                            platform = 'discord'
                        elif ':' in source:
                            platform = source.split(':')[0]

                    # Check RID for platform hints too
                    if 'rid' in content_data:
                        rid = content_data['rid']
                        if 'discourse.post' in rid:
                            platform = 'forum'

                    source_item = {
                        'platform': platform,
                        'type': content_data.get('source_type', 'document'),
                        'title': content_data.get('title', 'Untitled')[:100]
                    }

                    if created_at:
                        source_item['timestamp'] = created_at.isoformat()

                    if 'text' in content_data:
                        source_item['excerpt'] = content_data['text'][:200] + '...'

                    if 'url' in content_data:
                        source_item['url'] = content_data['url']

                    # Extract additional metadata if it exists
                    if meta and isinstance(meta, dict):
                        # Check for post-specific metadata
                        if 'post_number' in meta:
                            source_item['post_number'] = meta['post_number']
                            source_item['type'] = 'post'
                        if 'post_id' in meta:
                            source_item['post_id'] = meta['post_id']
                        if 'author' in meta:
                            source_item['author'] = meta['author']
                        if 'post_url' in meta:
                            source_item['url'] = meta['post_url']  # Override with specific URL
                        if 'created_at' in meta:
                            source_item['timestamp'] = meta['created_at']
                        if 'topic_title' in meta:
                            # For forum posts, include topic in title
                            source_item['title'] = f"{meta['topic_title']} - {source_item['title']}"

                    detailed_sources.append(source_item)

            # IMPORTANT: Also query koi_memories table where isolated sensor data is stored
            # This is where discourse sensor data actually goes when USE_ISOLATED_TABLES=true
            source_cur.execute("""
                SELECT rid, content, metadata, created_at
                FROM koi_memories
                WHERE created_at > NOW() - INTERVAL '3 days'
                AND metadata IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 100
            """)

            koi_memories = source_cur.fetchall()

            for memory_row in koi_memories:
                rid, content_data, meta, created_at = memory_row

                if meta and rid:
                    # Determine platform from RID
                    platform = 'unknown'
                    if 'discourse.post' in rid:
                        platform = 'forum'
                    elif 'telegram' in rid:
                        platform = 'telegram'
                    elif 'notion' in rid:
                        platform = 'notion'
                    elif 'twitter' in rid:
                        platform = 'twitter'
                    elif 'discord' in rid:
                        platform = 'discord'
                    elif 'website' in rid or 'web' in rid:
                        platform = 'website'

                    source_item = {
                        'platform': platform,
                        'type': meta.get('type', 'content'),
                        'title': meta.get('title', 'Untitled')[:100]
                    }

                    # Add URL - prioritize post_url from discourse sensor
                    if 'post_url' in meta:
                        source_item['url'] = meta['post_url']
                    elif 'url' in meta:
                        source_item['url'] = meta['url']
                    elif 'page_url' in meta:
                        source_item['url'] = meta['page_url']

                    # Add timestamp
                    if 'created_at' in meta:
                        source_item['timestamp'] = meta['created_at']
                    elif 'published_at' in meta:
                        source_item['timestamp'] = meta['published_at']
                    elif created_at:
                        source_item['timestamp'] = created_at.isoformat()

                    # Add author from discourse metadata
                    if 'author' in meta:
                        source_item['author'] = meta['author']
                    elif 'username' in meta:
                        source_item['author'] = meta['username']

                    # Add post-specific data from discourse sensor
                    if 'post_number' in meta:
                        source_item['post_number'] = meta['post_number']
                        source_item['type'] = 'post'
                    if 'post_id' in meta:
                        source_item['post_id'] = meta['post_id']
                    if 'topic_title' in meta:
                        source_item['title'] = f"{meta['topic_title']} - Post #{meta.get('post_number', '?')} by {meta.get('author', 'Unknown')}"
                    if 'reply_to_post_number' in meta:
                        source_item['reply_to'] = f"Post #{meta['reply_to_post_number']}"

                    # Extract content excerpt from JSONB content field
                    if content_data and isinstance(content_data, dict):
                        # Check for text in content JSONB
                        if 'text' in content_data:
                            source_item['excerpt'] = str(content_data['text'])[:200] + '...'
                        elif 'content' in content_data:
                            source_item['excerpt'] = str(content_data['content'])[:200] + '...'
                    elif 'content' in meta and meta['content']:
                        source_item['excerpt'] = str(meta['content'])[:200] + '...'
                    elif 'excerpt' in meta:
                        source_item['excerpt'] = meta['excerpt']

                    detailed_sources.append(source_item)

            source_cur.close()
            source_conn.close()

        except Exception as e:
            print(f"Warning: Could not extract real source data: {e}")
            # Fallback to basic info
            for source in provenance.get('sources', []):
                detailed_sources.append({
                    'platform': source.split(':')[0] if ':' in source else 'unknown',
                    'type': 'content',
                    'title': f"Content from {source}"
                })

        # Remove duplicates based on title and platform
        seen = set()
        unique_sources = []
        for source in detailed_sources:
            key = (source.get('platform'), source.get('title'))
            if key not in seen:
                seen.add(key)
                unique_sources.append(source)

        # Limit to reasonable number and assign to provenance
        provenance['detailed_sources'] = unique_sources[:min(25, max(10, provenance.get('source_count', 10)))]

        # Update source count based on actual sources found
        if provenance['detailed_sources']:
            provenance['source_count'] = max(provenance['source_count'], len(provenance['detailed_sources']))

        # Extract transformation pipeline info
        if 'pipeline' in metadata:
            provenance['transformation_pipeline'] = metadata['pipeline']

        # Extract CAT receipts if available
        if 'cat_receipts' in metadata:
            provenance['cat_receipts'] = metadata['cat_receipts']

    # Map sensor names to source URLs (for backward compatibility)
    sensor_to_url = {
        'discord-sensor': 'https://discord.gg/regen-network',
        'forum-sensor': 'https://forum.regen.network',
        'ledger-sensor': 'https://mainnet.regen.network',
        'governance-sensor': 'https://governance.regen.network',
        'twitter-sensor': 'https://twitter.com/RegenNetwork',
        'github-sensor': 'https://github.com/regen-network'
    }

    # Build source URLs from platforms
    for platform in provenance['platforms']:
        if platform in sensor_to_url:
            provenance['sources'].append(sensor_to_url[platform])
        elif platform and platform not in provenance['sources']:
            # Add the platform name directly if no URL mapping exists
            provenance['sources'].append(platform)

    # Ensure we have at least some source info
    if not provenance['sources'] and not provenance['platforms']:
        # Default sources if none detected
        provenance['platforms'] = ['forum', 'discord', 'ledger']
        provenance['sources'] = [
            'https://forum.regen.network',
            'https://discord.gg/regen-network',
            'https://mainnet.regen.network'
        ]

    # Update source count if we have source memories
    if provenance['source_memories']:
        provenance['source_count'] = max(provenance['source_count'], len(provenance['source_memories']))

    # For daily threads, extract sources from individual posts
    if content_type == 'daily_thread' and 'posts' in content:
        post_sources = []
        for post in content.get('posts', []):
            if 'sources' in post and post['sources']:
                for source in post['sources']:
                    if isinstance(source, dict):
                        # Extract source information
                        if source.get('type') == 'aggregated':
                            # Add sensor breakdown to platforms
                            if source.get('sensor_breakdown'):
                                for sensor in source['sensor_breakdown'].keys():
                                    if sensor not in provenance['platforms']:
                                        provenance['platforms'].append(sensor)
                        elif source.get('type') == 'ledger':
                            if 'ledger_sensor' not in provenance['platforms']:
                                provenance['platforms'].append('ledger_sensor')
                        else:
                            # Regular source with sensor, url, etc.
                            if source.get('sensor'):
                                if source['sensor'] not in provenance['platforms']:
                                    provenance['platforms'].append(source['sensor'])
                            if source.get('url'):
                                post_sources.append(source['url'])

        # Add unique post sources
        if post_sources:
            provenance['sources'].extend(post_sources)
            provenance['sources'] = list(set(provenance['sources']))  # Remove duplicates

    # For weekly digests, check for additional sources
    if content_type == 'weekly_digest' and 'sections' in content:
        # Count unique sources from weekly sections
        all_sources = set()
        for section in content.get('sections', []):
            if 'sources' in section:
                all_sources.update(section['sources'])
        if all_sources:
            provenance['sources'].extend(list(all_sources))
            provenance['sources'] = list(set(provenance['sources']))  # Remove duplicates
            provenance['source_count'] = max(provenance['source_count'], len(provenance['sources']))

    # Connect to database
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv('POSTGRES_PORT', 5433),
        database=os.getenv('POSTGRES_DB', 'eliza'),
        user=os.getenv('POSTGRES_USER', 'postgres'),
        password=os.getenv('POSTGRES_PASSWORD', 'postgres')
    )
    cur = conn.cursor()

    try:
        # Check if table has provenance column
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'quality_reviews' AND column_name = 'provenance'
        """)
        has_provenance_column = cur.fetchone() is not None

        if has_provenance_column:
            # Insert with provenance column
            cur.execute("""
                INSERT INTO quality_reviews (
                    review_id, content_id, content_type, content_data,
                    style_score, validation_score, quality_issues,
                    approval_status, reviewer_notes, reviewed_by,
                    auto_publish_eligible, created_at, provenance
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s
                )
            """, (
                str(uuid.uuid4()),  # review_id
                file_path,  # content_id (use filepath as ID)
                content_type,  # content_type
                json.dumps(content),  # content_data
                0.75,  # style_score (default)
                0.80,  # validation_score (default)
                json.dumps({"generated": True}),  # quality_issues
                'draft',  # approval_status - start as draft for review
                f'Generated at {datetime.now(timezone.utc)}',  # reviewer_notes
                'system',  # reviewed_by
                False,  # auto_publish_eligible
                datetime.now(timezone.utc),  # created_at
                json.dumps(provenance)  # provenance
            ))
        else:
            # Insert without provenance column (backward compatibility)
            cur.execute("""
                INSERT INTO quality_reviews (
                    review_id, content_id, content_type, content_data,
                    style_score, validation_score, quality_issues,
                    approval_status, reviewer_notes, reviewed_by,
                    auto_publish_eligible, created_at
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s
                )
            """, (
                str(uuid.uuid4()),  # review_id
                file_path,  # content_id (use filepath as ID)
                content_type,  # content_type
                json.dumps(content),  # content_data
                0.75,  # style_score (default)
                0.80,  # validation_score (default)
                json.dumps({"generated": True, "provenance": provenance}),  # quality_issues with provenance
                'draft',  # approval_status - start as draft for review
                f'Generated at {datetime.now(timezone.utc)}',  # reviewer_notes
                'system',  # reviewed_by
                False,  # auto_publish_eligible
                datetime.now(timezone.utc)  # created_at
            ))

        conn.commit()
        print(f"✓ Submitted {content_type} to review queue")
        return True

    except Exception as e:
        print(f"✗ Failed to submit to review: {e}")
        conn.rollback()
        return False
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python submit_to_review.py <file_path> <content_type>")
        print("  content_type: 'daily_thread' or 'weekly_digest'")
        sys.exit(1)

    submit_to_review(sys.argv[1], sys.argv[2])