"""
Quality Control System for Daily Bot and Weekly Digest
Implements comprehensive review and approval workflow for Milestone B
"""

import os
import json
import asyncio
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple
from enum import Enum
import yaml
from loguru import logger
import asyncpg


class ApprovalStatus(Enum):
    """Approval status for content"""
    DRAFT = "draft"
    PENDING_REVIEW = "pending_review"
    APPROVED = "approved"
    REJECTED = "rejected"
    PUBLISHED = "published"
    AUTO_PUBLISHED = "auto_published"
    ROLLED_BACK = "rolled_back"


class ContentType(Enum):
    """Type of content being reviewed"""
    DAILY_THREAD = "daily_thread"
    WEEKLY_DIGEST = "weekly_digest"
    PODCAST_BRIEF = "podcast_brief"


class QualityControl:
    """
    Main quality control system for content validation and approval
    Implements Milestone B requirements:
    - Content validation checks (no speculation, link validity)
    - Style guide compliance scoring
    - Approval interface for Gregory
    - Auto-publish logic after week 1
    - Rollback mechanisms
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize with configuration"""
        self.config = self._load_config(config_path)
        self.db_url = self.config.get('database_url', os.getenv('POSTGRES_URL', 'postgresql://postgres:postgres@localhost:5433/eliza'))
        
        # Quality thresholds
        self.thresholds = self.config.get('quality_thresholds', {})
        self.min_style_score = self.thresholds.get('min_style_score', 0.8)
        self.min_validation_score = self.thresholds.get('min_validation_score', 0.9)
        
        # Auto-publish settings
        self.auto_publish_config = self.config.get('auto_publish', {})
        self.auto_publish_enabled = self.auto_publish_config.get('enabled', False)
        self.auto_publish_after_days = self.auto_publish_config.get('after_days', 7)
        self.auto_publish_start_date = self.auto_publish_config.get('start_date', None)
        
        # Database connection pool
        self.pool = None
        
        # Rollback history
        self.rollback_history = []
        
        logger.info(f"Quality Control initialized (auto_publish={self.auto_publish_enabled})")
    
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if not config_path:
            config_path = Path(__file__).parent / "config" / "quality_config.yaml"
        
        if Path(config_path).exists():
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                logger.info(f"Loaded config from {config_path}")
                return config
        else:
            logger.warning("No config file found, using defaults")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration"""
        return {
            'database_url': os.getenv('POSTGRES_URL', 'postgresql://postgres:postgres@localhost:5433/eliza'),
            'quality_thresholds': {
                'min_style_score': 0.8,
                'min_validation_score': 0.9,
                'max_speculation_phrases': 0,
                'required_sources': True,
                'max_link_failures': 0
            },
            'auto_publish': {
                'enabled': False,
                'after_days': 7,
                'min_consecutive_approvals': 5,
                'quality_threshold': 0.85
            },
            'validation_rules': {
                'no_speculation': True,
                'verify_links': True,
                'check_sources': True,
                'professional_tone': True,
                'fact_checking': True,
                'no_private_data': True
            },
            'style_guide': {
                'tone': 'professional_friendly',
                'no_speculation': True,
                'require_sources': True,
                'david_fortson_rules': True
            }
        }
    
    async def initialize_db(self):
        """Initialize database connection and create tables"""
        try:
            self.pool = await asyncpg.create_pool(self.db_url)
            
            async with self.pool.acquire() as conn:
                # Create quality control tables
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS quality_reviews (
                        review_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        content_id VARCHAR(255) NOT NULL,
                        content_type VARCHAR(50) NOT NULL,
                        content_data JSONB NOT NULL,
                        style_score FLOAT NOT NULL,
                        validation_score FLOAT NOT NULL,
                        quality_issues JSONB,
                        approval_status VARCHAR(50) NOT NULL,
                        reviewer_notes TEXT,
                        reviewed_by VARCHAR(100),
                        auto_publish_eligible BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        reviewed_at TIMESTAMPTZ,
                        published_at TIMESTAMPTZ,
                        rolled_back_at TIMESTAMPTZ
                    );
                    
                    CREATE INDEX IF NOT EXISTS idx_reviews_content ON quality_reviews(content_id);
                    CREATE INDEX IF NOT EXISTS idx_reviews_status ON quality_reviews(approval_status);
                    CREATE INDEX IF NOT EXISTS idx_reviews_date ON quality_reviews(created_at);
                    
                    -- Table for tracking approval history
                    CREATE TABLE IF NOT EXISTS approval_history (
                        history_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        review_id UUID REFERENCES quality_reviews(review_id),
                        previous_status VARCHAR(50),
                        new_status VARCHAR(50),
                        changed_by VARCHAR(100),
                        notes TEXT,
                        changed_at TIMESTAMPTZ DEFAULT NOW()
                    );
                ''')
                
                logger.info("Quality control database initialized")
                
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            self.pool = None
    
    async def validate_content(self, 
                               content: Dict[str, Any], 
                               content_type: ContentType) -> Dict[str, Any]:
        """
        Perform comprehensive validation checks on content
        
        Args:
            content: Content to validate
            content_type: Type of content
            
        Returns:
            Validation results with scores and issues
        """
        validation_results = {
            'passed': True,
            'scores': {},
            'issues': [],
            'warnings': []
        }
        
        # Check for speculation
        if self.config['validation_rules'].get('no_speculation', True):
            spec_result = self._check_speculation(content)
            validation_results['scores']['speculation'] = spec_result['score']
            if spec_result['issues']:
                validation_results['issues'].extend(spec_result['issues'])
                validation_results['passed'] = False
        
        # Verify links
        if self.config['validation_rules'].get('verify_links', True):
            link_result = await self._verify_links(content)
            validation_results['scores']['links'] = link_result['score']
            if link_result['failures']:
                validation_results['issues'].extend(link_result['failures'])
                if len(link_result['failures']) > self.thresholds.get('max_link_failures', 0):
                    validation_results['passed'] = False
        
        # Check sources
        if self.config['validation_rules'].get('check_sources', True):
            source_result = self._check_sources(content)
            validation_results['scores']['sources'] = source_result['score']
            if not source_result['has_sources'] and self.thresholds.get('required_sources', True):
                validation_results['issues'].append("No sources cited")
                validation_results['passed'] = False
        
        # Check for private data
        if self.config['validation_rules'].get('no_private_data', True):
            private_result = self._check_private_data(content)
            if private_result['found']:
                validation_results['issues'].extend(private_result['issues'])
                validation_results['passed'] = False
        
        # Calculate overall validation score
        if validation_results['scores']:
            validation_results['overall_score'] = sum(validation_results['scores'].values()) / len(validation_results['scores'])
        else:
            validation_results['overall_score'] = 1.0
        
        # Check against minimum threshold
        if validation_results['overall_score'] < self.min_validation_score:
            validation_results['passed'] = False
            validation_results['issues'].append(
                f"Overall validation score {validation_results['overall_score']:.2f} below threshold {self.min_validation_score}"
            )
        
        logger.info(f"Content validation: passed={validation_results['passed']}, score={validation_results['overall_score']:.2f}")
        
        return validation_results
    
    def _check_speculation(self, content: Dict[str, Any]) -> Dict[str, Any]:
        """Check for speculative language"""
        speculation_phrases = [
            'might be', 'could be', 'possibly', 'potentially',
            'may lead to', 'expected to', 'likely to', 'probable',
            'rumors', 'unconfirmed', 'allegedly', 'supposedly',
            'we believe', 'we think', 'we expect', 'we predict',
            'should be', 'would be', 'speculate', 'forecast'
        ]
        
        issues = []
        found_count = 0
        
        # Check different content fields based on type
        text_to_check = ""
        if isinstance(content, dict):
            # For daily threads
            if 'posts' in content:
                for post in content.get('posts', []):
                    text_to_check += post.get('content', '') + " "
            # For weekly digests
            elif 'brief' in content:
                text_to_check = content.get('brief', '')
            # Generic text field
            elif 'text' in content:
                text_to_check = content.get('text', '')
        else:
            text_to_check = str(content)
        
        text_lower = text_to_check.lower()
        
        for phrase in speculation_phrases:
            if phrase in text_lower:
                found_count += 1
                issues.append(f"Found speculative phrase: '{phrase}'")
        
        score = 1.0 if found_count == 0 else max(0, 1.0 - (found_count * 0.2))
        
        return {
            'score': score,
            'issues': issues,
            'speculation_count': found_count
        }
    
    async def _verify_links(self, content: Dict[str, Any]) -> Dict[str, Any]:
        """Verify all links in content are valid"""
        import re
        import aiohttp
        
        # Extract links from content
        links = []
        text_to_check = ""
        
        if isinstance(content, dict):
            if 'posts' in content:
                for post in content.get('posts', []):
                    text_to_check += post.get('content', '') + " "
            elif 'brief' in content:
                text_to_check = content.get('brief', '')
            elif 'links' in content:
                links.extend(content.get('links', []))
        
        # Extract URLs using regex
        url_pattern = r'https?://[^\s<>"{}\\|\^\[\]`]+'
        found_urls = re.findall(url_pattern, text_to_check)
        links.extend(found_urls)
        
        # Remove duplicates
        links = list(set(links))
        
        failures = []
        success_count = 0
        
        # Verify each link
        async with aiohttp.ClientSession() as session:
            for link in links:
                try:
                    async with session.head(link, timeout=5, allow_redirects=True) as response:
                        if response.status >= 400:
                            failures.append(f"Link returned {response.status}: {link}")
                        else:
                            success_count += 1
                except Exception as e:
                    failures.append(f"Failed to verify link: {link} ({str(e)})")
        
        total_links = len(links)
        score = success_count / total_links if total_links > 0 else 1.0
        
        return {
            'score': score,
            'total_links': total_links,
            'valid_links': success_count,
            'failures': failures
        }
    
    def _check_sources(self, content: Dict[str, Any]) -> Dict[str, Any]:
        """Check if content has proper source citations"""
        source_indicators = [
            'according to', 'source:', 'via', 'from',
            'reported by', 'announced by', 'published by',
            'data from', 'statistics from', 'based on'
        ]
        
        text_to_check = ""
        if isinstance(content, dict):
            if 'posts' in content:
                for post in content.get('posts', []):
                    text_to_check += post.get('content', '') + " "
            elif 'brief' in content:
                text_to_check = content.get('brief', '')
            elif 'sources' in content:
                # Direct sources field
                return {
                    'score': 1.0,
                    'has_sources': len(content.get('sources', [])) > 0,
                    'source_count': len(content.get('sources', []))
                }
        
        text_lower = text_to_check.lower()
        
        # Check for source indicators
        found_sources = False
        for indicator in source_indicators:
            if indicator in text_lower:
                found_sources = True
                break
        
        # Also check for URLs as implicit sources
        import re
        url_pattern = r'https?://[^\s]+'
        urls = re.findall(url_pattern, text_to_check)
        if urls:
            found_sources = True
        
        return {
            'score': 1.0 if found_sources else 0.0,
            'has_sources': found_sources,
            'source_count': len(urls)
        }
    
    def _check_private_data(self, content: Dict[str, Any]) -> Dict[str, Any]:
        """Check for potential private or sensitive data"""
        import re
        
        issues = []
        patterns = {
            'email': r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
            'phone': r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b',
            'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
            'api_key': r'\b[A-Za-z0-9]{32,}\b',
            'private_key': r'-----BEGIN [A-Z ]+ KEY-----'
        }
        
        text_to_check = json.dumps(content) if isinstance(content, dict) else str(content)
        
        for data_type, pattern in patterns.items():
            if re.search(pattern, text_to_check):
                issues.append(f"Potential {data_type} found")
        
        return {
            'found': len(issues) > 0,
            'issues': issues
        }
    
    async def calculate_style_score(self, content: Dict[str, Any]) -> float:
        """
        Calculate style guide compliance score
        
        Args:
            content: Content to score
            
        Returns:
            Style score between 0 and 1
        """
        # Import style enforcer if available
        try:
            import sys
            from pathlib import Path
            sys.path.append(str(Path(__file__).parent.parent / "koi-sensors"))
            from bots.components.style_enforcer import StyleEnforcer
            
            # Use existing style enforcer
            enforcer = StyleEnforcer(self.config)
            result = enforcer.enforce_style(content)
            return result.get('style_score', 0.0)
            
        except ImportError:
            # Fallback to basic style checking
            logger.warning("StyleEnforcer not available, using basic style checking")
            
            score = 1.0
            text_to_check = ""
            
            if isinstance(content, dict):
                if 'posts' in content:
                    for post in content.get('posts', []):
                        text_to_check += post.get('content', '') + " "
                elif 'brief' in content:
                    text_to_check = content.get('brief', '')
            
            # Check for all caps (reduces score)
            if text_to_check.isupper() and len(text_to_check) > 10:
                score -= 0.3
            
            # Check for excessive punctuation
            if '!!!' in text_to_check or '???' in text_to_check:
                score -= 0.2
            
            # Check for professional tone indicators
            professional_words = ['announced', 'confirmed', 'launched', 'published']
            has_professional = any(word in text_to_check.lower() for word in professional_words)
            if not has_professional:
                score -= 0.1
            
            return max(0, score)
    
    async def submit_for_review(self,
                                content: Dict[str, Any],
                                content_type: ContentType,
                                content_id: Optional[str] = None) -> str:
        """
        Submit content for quality review
        
        Args:
            content: Content to review
            content_type: Type of content
            content_id: Optional ID for the content
            
        Returns:
            Review ID
        """
        # Generate content ID if not provided
        if not content_id:
            import uuid
            content_id = str(uuid.uuid4())
        
        # Perform validation
        validation_results = await self.validate_content(content, content_type)
        
        # Calculate style score
        style_score = await self.calculate_style_score(content)
        
        # Determine initial status
        if validation_results['passed'] and style_score >= self.min_style_score:
            initial_status = ApprovalStatus.PENDING_REVIEW.value
            # Check if eligible for auto-publish
            auto_publish_eligible = await self._check_auto_publish_eligibility()
        else:
            initial_status = ApprovalStatus.DRAFT.value
            auto_publish_eligible = False
        
        # Store review in database
        review_id = await self._store_review(
            content_id=content_id,
            content_type=content_type.value,
            content_data=content,
            style_score=style_score,
            validation_score=validation_results['overall_score'],
            quality_issues={
                'validation': validation_results,
                'style_score': style_score
            },
            approval_status=initial_status,
            auto_publish_eligible=auto_publish_eligible
        )
        
        logger.info(f"Content submitted for review: {review_id} (status={initial_status})")
        
        return review_id
    
    async def _check_auto_publish_eligibility(self) -> bool:
        """
        Check if content is eligible for auto-publish based on history
        
        Returns:
            True if eligible for auto-publish
        """
        if not self.auto_publish_enabled:
            return False
        
        # Check if we're past the initial approval period
        if self.auto_publish_start_date:
            start_date = datetime.fromisoformat(self.auto_publish_start_date)
            days_since_start = (datetime.now(timezone.utc) - start_date).days
            if days_since_start < self.auto_publish_after_days:
                return False
        
        # Check recent approval history
        if self.pool:
            try:
                async with self.pool.acquire() as conn:
                    # Get recent approvals
                    recent_approvals = await conn.fetchval('''
                        SELECT COUNT(*) FROM quality_reviews
                        WHERE approval_status = $1
                        AND created_at > $2
                        AND validation_score >= $3
                        AND style_score >= $4
                    ''',
                    ApprovalStatus.APPROVED.value,
                    datetime.now(timezone.utc) - timedelta(days=7),
                    self.auto_publish_config.get('quality_threshold', 0.85),
                    self.auto_publish_config.get('quality_threshold', 0.85)
                    )
                    
                    min_approvals = self.auto_publish_config.get('min_consecutive_approvals', 5)
                    return recent_approvals >= min_approvals
                    
            except Exception as e:
                logger.error(f"Failed to check auto-publish eligibility: {e}")
        
        return False
    
    async def _store_review(self, **kwargs) -> str:
        """
        Store review in database
        
        Returns:
            Review ID
        """
        import uuid
        review_id = str(uuid.uuid4())
        
        if self.pool is None:
            await self.initialize_db()
        
        if self.pool:
            try:
                async with self.pool.acquire() as conn:
                    await conn.execute('''
                        INSERT INTO quality_reviews
                        (review_id, content_id, content_type, content_data,
                         style_score, validation_score, quality_issues,
                         approval_status, auto_publish_eligible, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ''',
                    uuid.UUID(review_id),
                    kwargs['content_id'],
                    kwargs['content_type'],
                    json.dumps(kwargs['content_data']),
                    kwargs['style_score'],
                    kwargs['validation_score'],
                    json.dumps(kwargs['quality_issues']),
                    kwargs['approval_status'],
                    kwargs.get('auto_publish_eligible', False),
                    datetime.now(timezone.utc)
                    )
                    
            except Exception as e:
                logger.error(f"Failed to store review: {e}")
        
        return review_id
    
    async def get_review(self, review_id: str) -> Optional[Dict[str, Any]]:
        """
        Get review details
        
        Args:
            review_id: Review ID
            
        Returns:
            Review data or None
        """
        if self.pool:
            try:
                async with self.pool.acquire() as conn:
                    row = await conn.fetchrow('''
                        SELECT * FROM quality_reviews WHERE review_id = $1
                    ''', uuid.UUID(review_id))
                    
                    if row:
                        return {
                            'review_id': str(row['review_id']),
                            'content_id': row['content_id'],
                            'content_type': row['content_type'],
                            'content_data': json.loads(row['content_data']),
                            'style_score': row['style_score'],
                            'validation_score': row['validation_score'],
                            'quality_issues': json.loads(row['quality_issues']) if row['quality_issues'] else {},
                            'approval_status': row['approval_status'],
                            'reviewer_notes': row['reviewer_notes'],
                            'reviewed_by': row['reviewed_by'],
                            'auto_publish_eligible': row['auto_publish_eligible'],
                            'created_at': row['created_at'].isoformat(),
                            'reviewed_at': row['reviewed_at'].isoformat() if row['reviewed_at'] else None,
                            'published_at': row['published_at'].isoformat() if row['published_at'] else None
                        }
                        
            except Exception as e:
                logger.error(f"Failed to get review: {e}")
        
        return None
    
    async def approve_content(self,
                             review_id: str,
                             reviewer: str = "Gregory",
                             notes: str = "") -> bool:
        """
        Approve content for publication
        
        Args:
            review_id: Review ID
            reviewer: Name of reviewer
            notes: Approval notes
            
        Returns:
            Success status
        """
        return await self._update_review_status(
            review_id=review_id,
            new_status=ApprovalStatus.APPROVED,
            reviewer=reviewer,
            notes=notes
        )
    
    async def reject_content(self,
                            review_id: str,
                            reviewer: str = "Gregory",
                            notes: str = "") -> bool:
        """
        Reject content
        
        Args:
            review_id: Review ID
            reviewer: Name of reviewer
            notes: Rejection reason
            
        Returns:
            Success status
        """
        return await self._update_review_status(
            review_id=review_id,
            new_status=ApprovalStatus.REJECTED,
            reviewer=reviewer,
            notes=notes
        )
    
    async def _update_review_status(self,
                                   review_id: str,
                                   new_status: ApprovalStatus,
                                   reviewer: str,
                                   notes: str) -> bool:
        """
        Update review status
        
        Returns:
            Success status
        """
        if self.pool:
            try:
                async with self.pool.acquire() as conn:
                    # Get current status
                    current_status = await conn.fetchval('''
                        SELECT approval_status FROM quality_reviews WHERE review_id = $1
                    ''', uuid.UUID(review_id))
                    
                    if not current_status:
                        logger.error(f"Review {review_id} not found")
                        return False
                    
                    # Update review
                    timestamp = datetime.now(timezone.utc)
                    await conn.execute('''
                        UPDATE quality_reviews
                        SET approval_status = $1, reviewer_notes = $2, reviewed_by = $3,
                            reviewed_at = $4,
                            published_at = CASE WHEN $1 IN ('published', 'auto_published') THEN $4 ELSE published_at END
                        WHERE review_id = $5
                    ''',
                    new_status.value, notes, reviewer, timestamp, uuid.UUID(review_id)
                    )
                    
                    # Add to history
                    await conn.execute('''
                        INSERT INTO approval_history
                        (review_id, previous_status, new_status, changed_by, notes, changed_at)
                        VALUES ($1, $2, $3, $4, $5, $6)
                    ''',
                    uuid.UUID(review_id), current_status, new_status.value,
                    reviewer, notes, timestamp
                    )
                    
                    logger.info(f"Updated review {review_id}: {current_status} -> {new_status.value}")
                    return True
                    
            except Exception as e:
                logger.error(f"Failed to update review status: {e}")
        
        return False
    
    async def auto_publish_check(self) -> List[str]:
        """
        Check for content eligible for auto-publish and publish it
        
        Returns:
            List of auto-published review IDs
        """
        published = []
        
        if not self.auto_publish_enabled:
            return published
        
        if self.pool:
            try:
                async with self.pool.acquire() as conn:
                    # Find eligible content
                    rows = await conn.fetch('''
                        SELECT review_id FROM quality_reviews
                        WHERE approval_status = $1
                        AND auto_publish_eligible = true
                        AND validation_score >= $2
                        AND style_score >= $3
                        ORDER BY created_at
                        LIMIT 10
                    ''',
                    ApprovalStatus.APPROVED.value,
                    self.auto_publish_config.get('quality_threshold', 0.85),
                    self.auto_publish_config.get('quality_threshold', 0.85)
                    )
                    
                    for row in rows:
                        review_id = str(row['review_id'])
                        success = await self._update_review_status(
                            review_id=review_id,
                            new_status=ApprovalStatus.AUTO_PUBLISHED,
                            reviewer="AutoPublish",
                            notes="Automatically published based on quality history"
                        )
                        if success:
                            published.append(review_id)
                    
            except Exception as e:
                logger.error(f"Failed to auto-publish: {e}")
        
        if published:
            logger.info(f"Auto-published {len(published)} items: {published}")
        
        return published
    
    async def rollback_publication(self,
                                  review_id: str,
                                  reason: str,
                                  rolled_back_by: str = "System") -> bool:
        """
        Rollback a published item
        
        Args:
            review_id: Review ID to rollback
            reason: Reason for rollback
            rolled_back_by: Who initiated the rollback
            
        Returns:
            Success status
        """
        if self.pool:
            try:
                async with self.pool.acquire() as conn:
                    # Get current content
                    row = await conn.fetchrow('''
                        SELECT content_data, approval_status FROM quality_reviews
                        WHERE review_id = $1
                    ''', uuid.UUID(review_id))
                    
                    if not row:
                        logger.error(f"Review {review_id} not found")
                        return False
                    
                    if row['approval_status'] not in ['published', 'auto_published']:
                        logger.error(f"Review {review_id} is not published, cannot rollback")
                        return False
                    
                    # Store rollback info
                    self.rollback_history.append({
                        'review_id': review_id,
                        'content': json.loads(row['content_data']),
                        'reason': reason,
                        'rolled_back_by': rolled_back_by,
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    })
                    
                    # Update status
                    success = await self._update_review_status(
                        review_id=review_id,
                        new_status=ApprovalStatus.ROLLED_BACK,
                        reviewer=rolled_back_by,
                        notes=f"Rolled back: {reason}"
                    )
                    
                    if success:
                        # Update rolled_back_at timestamp
                        await conn.execute('''
                            UPDATE quality_reviews
                            SET rolled_back_at = $1
                            WHERE review_id = $2
                        ''', datetime.now(timezone.utc), uuid.UUID(review_id))
                        
                        logger.info(f"Rolled back publication: {review_id}")
                    
                    return success
                    
            except Exception as e:
                logger.error(f"Failed to rollback publication: {e}")
        
        return False
    
    async def get_pending_reviews(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get list of pending reviews for approval
        
        Args:
            limit: Maximum number of reviews to return
            
        Returns:
            List of pending reviews
        """
        reviews = []
        
        if self.pool:
            try:
                async with self.pool.acquire() as conn:
                    rows = await conn.fetch('''
                        SELECT review_id, content_id, content_type, style_score,
                               validation_score, created_at, auto_publish_eligible
                        FROM quality_reviews
                        WHERE approval_status = $1
                        ORDER BY created_at DESC
                        LIMIT $2
                    ''', ApprovalStatus.PENDING_REVIEW.value, limit)
                    
                    for row in rows:
                        reviews.append({
                            'review_id': str(row['review_id']),
                            'content_id': row['content_id'],
                            'content_type': row['content_type'],
                            'style_score': row['style_score'],
                            'validation_score': row['validation_score'],
                            'created_at': row['created_at'].isoformat(),
                            'auto_publish_eligible': row['auto_publish_eligible']
                        })
                        
            except Exception as e:
                logger.error(f"Failed to get pending reviews: {e}")
        
        return reviews
    
    async def get_approval_stats(self, days: int = 7) -> Dict[str, Any]:
        """
        Get approval statistics for the specified period
        
        Args:
            days: Number of days to look back
            
        Returns:
            Statistics dictionary
        """
        stats = {
            'total_reviews': 0,
            'approved': 0,
            'rejected': 0,
            'published': 0,
            'auto_published': 0,
            'rolled_back': 0,
            'pending': 0,
            'avg_style_score': 0,
            'avg_validation_score': 0
        }
        
        if self.pool:
            try:
                async with self.pool.acquire() as conn:
                    since = datetime.now(timezone.utc) - timedelta(days=days)
                    
                    # Get counts by status
                    rows = await conn.fetch('''
                        SELECT approval_status, COUNT(*) as count
                        FROM quality_reviews
                        WHERE created_at > $1
                        GROUP BY approval_status
                    ''', since)
                    
                    for row in rows:
                        status = row['approval_status']
                        count = row['count']
                        stats['total_reviews'] += count
                        
                        if status == ApprovalStatus.APPROVED.value:
                            stats['approved'] = count
                        elif status == ApprovalStatus.REJECTED.value:
                            stats['rejected'] = count
                        elif status == ApprovalStatus.PUBLISHED.value:
                            stats['published'] = count
                        elif status == ApprovalStatus.AUTO_PUBLISHED.value:
                            stats['auto_published'] = count
                        elif status == ApprovalStatus.ROLLED_BACK.value:
                            stats['rolled_back'] = count
                        elif status == ApprovalStatus.PENDING_REVIEW.value:
                            stats['pending'] = count
                    
                    # Get average scores
                    avg_row = await conn.fetchrow('''
                        SELECT AVG(style_score) as avg_style,
                               AVG(validation_score) as avg_validation
                        FROM quality_reviews
                        WHERE created_at > $1
                    ''', since)
                    
                    if avg_row:
                        stats['avg_style_score'] = float(avg_row['avg_style'] or 0)
                        stats['avg_validation_score'] = float(avg_row['avg_validation'] or 0)
                        
            except Exception as e:
                logger.error(f"Failed to get approval stats: {e}")
        
        return stats
    
    async def cleanup(self):
        """Clean up database connections"""
        if self.pool:
            await self.pool.close()
            logger.info("Quality control database connection closed")