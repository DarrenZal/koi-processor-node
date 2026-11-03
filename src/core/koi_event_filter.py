#!/usr/bin/env python3
"""
KOI Event Filter - Filters out non-content events before processing
"""

import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class KOIEventFilter:
    """Filters events to prevent non-content from being processed"""

    # Event types that should be processed
    CONTENT_EVENT_TYPES = {
        'content_created',
        'content_updated',
        'document_added',
        'page_created',
        'page_updated',
        'post_created',
        'post_updated',
        'article_published',
        'message_sent',
        'issue_created',
        'issue_updated',
        'pr_created',
        'pr_merged',
        'commit_pushed'
    }

    # RID patterns that indicate non-content
    NON_CONTENT_PATTERNS = [
        'heartbeat',
        'ping',
        'health_check',
        'status_update',
        'monitoring'
    ]

    @classmethod
    def should_process_event(cls, event: Dict[str, Any]) -> bool:
        """
        Determine if an event should be processed for content extraction.

        Returns True if event contains real content, False otherwise.
        """
        # Check if it's a heartbeat
        if cls._is_heartbeat(event):
            logger.debug(f"Filtered out heartbeat event: {event.get('rid', 'unknown')}")
            return False

        # Check if it's test data
        if cls._is_test_data(event):
            logger.debug(f"Filtered out test data: {event.get('rid', 'unknown')}")
            return False

        # Check if it's monitoring/status data
        if cls._is_monitoring_data(event):
            logger.debug(f"Filtered out monitoring data: {event.get('rid', 'unknown')}")
            return False

        # Check if event type indicates content
        event_type = event.get('event_type', '')
        event_type_lower = event_type.lower()

        # Accept "NEW" and "UPDATE" events as they contain real content
        if event_type in ['NEW', 'UPDATE', 'new', 'update']:
            # But still filter if it's a heartbeat
            if not cls._is_heartbeat(event):
                return True

        if event_type_lower in cls.CONTENT_EVENT_TYPES:
            return True

        # Check bundle content
        bundle = event.get('bundle', {})
        if bundle:
            content = bundle.get('contents', {})
            # Check if content has actual text or data
            if cls._has_meaningful_content(content):
                return True

        # Default to not processing if unclear
        logger.info(f"Event type '{event_type}' not in whitelist, filtering out: {event.get('rid', 'unknown')}")
        return False

    @classmethod
    def _is_heartbeat(cls, event: Dict[str, Any]) -> bool:
        """Check if event is a heartbeat"""
        # Check RID
        rid = event.get('rid', '').lower()
        if 'heartbeat' in rid:
            return True

        # Check bundle content
        bundle = event.get('bundle', {})
        if bundle:
            content = bundle.get('contents', {})
            if isinstance(content, dict):
                # Check for sensor_heartbeat type
                if content.get('type') == 'sensor_heartbeat':
                    return True
                # Check text field
                text = content.get('text', '')
                if isinstance(text, str) and 'sensor_heartbeat' in text:
                    return True

        return False

    @classmethod
    def _is_test_data(cls, event: Dict[str, Any]) -> bool:
        """Check if event is test data"""
        rid = event.get('rid', '').lower()
        # Be more specific - only filter if test/demo is in the sensor name or as a standalone term
        source_node = event.get('source_node', '').lower()

        # Check if it's from a test sensor
        if 'test-sensor' in source_node:
            # But allow if it's from our deduplication test
            if 'koi:test-sensor' == source_node:
                return False  # Allow our test sensor for testing deduplication

        # Filter obvious test data patterns
        if '_test_' in rid or rid.startswith('test_') or rid.endswith('_test'):
            return True
        if '_demo_' in rid or rid.startswith('demo_') or rid.endswith('_demo'):
            return True

        return False

    @classmethod
    def _is_monitoring_data(cls, event: Dict[str, Any]) -> bool:
        """Check if event is monitoring/status data"""
        rid = event.get('rid', '').lower()
        source_node = event.get('source_node', '').lower()

        # Allow our test sensor for deduplication testing
        if source_node == 'koi:test-sensor':
            return False

        for pattern in cls.NON_CONTENT_PATTERNS:
            if pattern in rid:
                return True
        return False

    @classmethod
    def _has_meaningful_content(cls, content: Dict[str, Any]) -> bool:
        """Check if content has meaningful text or data"""
        if not content:
            return False

        # Check for text content
        text = content.get('text', '')
        if isinstance(text, str) and len(text) > 50:
            # But not if it's a heartbeat JSON
            if 'sensor_heartbeat' not in text:
                return True

        # Check for other content fields
        if content.get('body') or content.get('description') or content.get('content'):
            return True

        # Check for URL (might be a real page to process)
        if content.get('url') and not any(p in str(content.get('url', '')).lower() for p in ['heartbeat', 'health', 'status']):
            return True

        return False


# Integration function for Event Bridge
def filter_koi_event(event_dict: Dict[str, Any]) -> bool:
    """
    Main function to integrate with KOI Event Bridge.
    Returns True if event should be processed, False if it should be filtered.
    """
    return KOIEventFilter.should_process_event(event_dict)
