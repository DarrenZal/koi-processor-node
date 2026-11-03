#!/usr/bin/env python3
"""
Podcast Generation Job Queue
Manages async podcast generation jobs with status tracking
"""

import threading
import time
import json
import os
import subprocess
from datetime import datetime, timezone
from typing import Dict, Optional, Any
from enum import Enum
from loguru import logger
import psycopg2
from psycopg2.extras import RealDictCursor


class JobStatus(Enum):
    """Job status enum"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class PodcastJobQueue:
    """Simple in-memory job queue for podcast generation"""

    def __init__(self, db_config: Dict[str, Any]):
        self.jobs: Dict[str, Dict[str, Any]] = {}
        self.db_config = db_config
        self.lock = threading.Lock()

    def submit_job(self, draft_id: str, content: Dict[str, Any]) -> str:
        """Submit a new podcast generation job"""
        job_id = f"podcast_{draft_id}_{int(time.time())}"

        with self.lock:
            self.jobs[job_id] = {
                'job_id': job_id,
                'draft_id': draft_id,
                'status': JobStatus.PENDING.value,
                'progress': 0,
                'message': 'Job queued',
                'created_at': datetime.now(timezone.utc).isoformat(),
                'started_at': None,
                'completed_at': None,
                'error': None,
                'result': None
            }

        # Start processing in background thread
        thread = threading.Thread(
            target=self._process_job,
            args=(job_id, draft_id, content),
            daemon=True
        )
        thread.start()

        logger.info(f"Submitted podcast job {job_id} for draft {draft_id}")
        return job_id

    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get current job status"""
        with self.lock:
            return self.jobs.get(job_id)

    def _update_job(self, job_id: str, **kwargs):
        """Update job status (thread-safe)"""
        with self.lock:
            if job_id in self.jobs:
                self.jobs[job_id].update(kwargs)

    def _process_job(self, job_id: str, draft_id: str, content: Dict[str, Any]):
        """Process podcast generation job in background"""
        try:
            self._update_job(
                job_id,
                status=JobStatus.PROCESSING.value,
                started_at=datetime.now(timezone.utc).isoformat(),
                progress=10,
                message='Generating podcast script...'
            )

            # Generate podcast script
            podcast_text = self._generate_podcast_script(content)

            self._update_job(job_id, progress=30, message='Saving content...')

            # Save content to temp file
            temp_file = f'/tmp/weekly_digest_for_audio_{draft_id}.json'
            with open(temp_file, 'w') as f:
                content['podcast_script'] = podcast_text
                json.dump(content, f)

            self._update_job(job_id, progress=50, message='Generating audio (this may take a few minutes)...')

            # Run audio generation using venv Python
            env = os.environ.copy()

            # Use venv python if available, otherwise system python
            python_path = '/opt/projects/koi-processor/venv/bin/python3'
            if not os.path.exists(python_path):
                python_path = 'python3'

            cmd = [
                python_path,
                '/opt/projects/koi-processor/src/audio/simple_podcast_generator.py',
                temp_file,
                '--output-dir', '/opt/projects/koi-processor/podcast_audio'
            ]

            logger.info(f"Running podcast generation: {' '.join(cmd)}")

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd='/opt/projects/koi-processor',
                env=env,
                timeout=300  # 5 minutes max
            )

            if result.returncode == 0:
                self._update_job(job_id, progress=80, message='Processing output...')

                # Parse output to find generated file
                audio_file = None
                file_size = None

                if '✅ Podcast generated:' in result.stdout:
                    for line in result.stdout.split('\n'):
                        if '✅ Podcast generated:' in line:
                            audio_file = line.split('✅ Podcast generated:')[1].strip()
                        if '📊 File size:' in line:
                            file_size = line.split('📊 File size:')[1].strip()

                # Generate markdown
                self._update_job(job_id, progress=90, message='Generating markdown...')
                markdown = self._generate_weekly_markdown(content)
                markdown_file = f'/opt/projects/koi-processor/podcast_audio/weekly_digest_{draft_id[:8]}.md'
                with open(markdown_file, 'w') as f:
                    f.write(markdown)

                # Update database
                self._update_job(job_id, progress=95, message='Updating database...')
                self._update_draft_in_db(draft_id, podcast_text, audio_file)

                # Job completed successfully
                self._update_job(
                    job_id,
                    status=JobStatus.COMPLETED.value,
                    progress=100,
                    message='Podcast generated successfully!',
                    completed_at=datetime.now(timezone.utc).isoformat(),
                    result={
                        'audio_file': audio_file,
                        'file_size': file_size,
                        'script_length': len(podcast_text),
                        'markdown_file': f'weekly_digest_{draft_id[:8]}.md'
                    }
                )

                logger.info(f"Podcast job {job_id} completed successfully")

            else:
                raise Exception(result.stderr or 'Podcast generation failed')

        except Exception as e:
            logger.error(f"Podcast job {job_id} failed: {e}")
            self._update_job(
                job_id,
                status=JobStatus.FAILED.value,
                completed_at=datetime.now(timezone.utc).isoformat(),
                error=str(e),
                message=f'Failed: {str(e)}'
            )

    def _generate_podcast_script(self, content: Dict[str, Any]) -> str:
        """Generate a 20-minute podcast script from weekly digest content"""
        brief = content.get('brief_content', content.get('brief', ''))
        themes = content.get('themes', {})

        script = f"""
# Weekly Podcast Script
## Duration: ~20 minutes

### Opening (2 minutes)
Welcome to the Regen Network Weekly Podcast, where we explore the latest developments in regenerative finance and ecological economics. This week, we're covering the period from {content.get('week_start', 'this week')}.

### Main Content (15 minutes)
{brief}

### Key Themes Discussion (2 minutes)
Let's dive deeper into the key themes that emerged this week:
"""

        for theme, topics in themes.items():
            script += f"\n- {theme}: {', '.join(topics) if isinstance(topics, list) else topics}"

        script += """

### Closing (1 minute)
That's all for this week's Regen Network podcast. Join us next week as we continue to explore the cutting edge of regenerative economics and blockchain innovation. Until then, stay regenerative!
"""

        return script

    def _generate_weekly_markdown(self, content: Dict[str, Any]) -> str:
        """Generate markdown for NotebookLM - simplified version"""
        markdown = "# Regen Network Weekly Digest\n\n"

        if content.get('week_start'):
            markdown += f"**Week of:** {content['week_start']}\n\n"

        if content.get('executive_summary'):
            markdown += "## Executive Summary\n\n"
            markdown += content['executive_summary'] + "\n\n"

        if content.get('brief'):
            markdown += "## Weekly Brief\n\n"
            markdown += content['brief'] + "\n\n"

        markdown += "\n---\n*Generated by Regen Network KOI System*\n"
        return markdown

    def _update_draft_in_db(self, draft_id: str, podcast_text: str, audio_file: Optional[str]):
        """Update draft in database with podcast metadata"""
        try:
            conn = psycopg2.connect(**self.db_config, cursor_factory=RealDictCursor)
            cur = conn.cursor()

            update_data = {
                'podcast_script': podcast_text,
                'audio_file': audio_file
            }

            cur.execute("""
                UPDATE quality_reviews
                SET
                    quality_issues = jsonb_set(
                        COALESCE(quality_issues, '{}'::jsonb),
                        '{podcast_generated}',
                        'true'
                    ),
                    content_data = content_data || %s::jsonb
                WHERE review_id = %s
            """, (json.dumps(update_data), draft_id))

            conn.commit()
            cur.close()
            conn.close()

        except Exception as e:
            logger.error(f"Failed to update draft {draft_id} in database: {e}")
            raise


# Global job queue instance
_job_queue = None


def get_job_queue(db_config: Dict[str, Any]) -> PodcastJobQueue:
    """Get or create global job queue instance"""
    global _job_queue
    if _job_queue is None:
        _job_queue = PodcastJobQueue(db_config)
    return _job_queue
