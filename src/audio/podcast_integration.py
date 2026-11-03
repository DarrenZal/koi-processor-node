#!/usr/bin/env python3
"""
Podcast Integration Script
Connects Weekly Aggregator → Podcastfy Audio Generation → Podcast Publisher

This script orchestrates the complete weekly podcast generation pipeline:
1. Runs weekly aggregator to create digest
2. Generates audio using Podcastfy (automated) or NotebookLM (manual fallback)
3. Publishes podcast episode with RSS feed

Primary mode: Automated with Podcastfy
Fallback mode: Manual with NotebookLM export
"""

import os
import sys
import json
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any

# Import our modules
try:
    from weekly_aggregator import WeeklyAggregator
    from podcast_publisher import PodcastPublisher
    from audio_pipeline_enhanced import EnhancedAudioPipeline
except ImportError as e:
    print(f"Warning: Could not import module: {e}")
    print("Make sure all required modules are in the same directory")
    sys.exit(1)

# Try to import Podcastfy generator (primary method)
try:
    from podcastfy_generator import PodcastfyGenerator
    PODCASTFY_AVAILABLE = True
except ImportError:
    PODCASTFY_AVAILABLE = False
    print("Warning: Podcastfy generator not available. Will use NotebookLM fallback.")

# Try to import NotebookLM exporter (fallback method)
try:
    from notebooklm_exporter import NotebookLMExporter
    NOTEBOOKLM_AVAILABLE = True
except ImportError:
    NOTEBOOKLM_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PodcastIntegration:
    """
    Integrates the complete podcast generation pipeline
    """
    
    def __init__(self, config_path: str = "config/podcast_integration.json"):
        """Initialize the integration pipeline"""
        self.config = self._load_config(config_path)
        
        # Initialize components
        self.weekly_aggregator = WeeklyAggregator()
        self.podcast_publisher = PodcastPublisher()
        self.audio_pipeline = EnhancedAudioPipeline()
        
        # Initialize audio generation method
        self.audio_method = self.config.get("audio_method", "auto")  # auto, podcastfy, notebooklm
        
        if self.audio_method == "auto":
            if PODCASTFY_AVAILABLE:
                self.audio_method = "podcastfy"
                self.podcastfy_generator = PodcastfyGenerator()
                logger.info("Using Podcastfy for automated audio generation")
            elif NOTEBOOKLM_AVAILABLE:
                self.audio_method = "notebooklm"
                self.notebooklm_exporter = NotebookLMExporter()
                logger.info("Using NotebookLM export (manual process required)")
            else:
                raise ImportError("No audio generation method available. Install podcastfy or ensure notebooklm_exporter.py exists")
        elif self.audio_method == "podcastfy":
            if not PODCASTFY_AVAILABLE:
                raise ImportError("Podcastfy requested but not available. Install with: pip install podcastfy")
            self.podcastfy_generator = PodcastfyGenerator()
        elif self.audio_method == "notebooklm":
            if not NOTEBOOKLM_AVAILABLE:
                raise ImportError("NotebookLM exporter requested but not available")
            self.notebooklm_exporter = NotebookLMExporter()
        
        # Paths
        self.output_path = Path(self.config.get("output_path", "./podcast_output"))
        self.watch_path = Path(self.config.get("watch_path", "./notebooklm_audio"))
        
        # Create directories
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.watch_path.mkdir(parents=True, exist_ok=True)
    
    def _load_config(self, config_path: str) -> Dict:
        """Load or create default configuration"""
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
        else:
            # Default configuration
            default_config = {
                "output_path": "./podcast_output",
                "watch_path": "./notebooklm_audio",
                "auto_publish": False,
                "watch_timeout": 3600,  # 1 hour timeout for audio file
                "min_audio_duration": 900,  # Minimum 15 minutes
                "max_audio_duration": 1800,  # Maximum 30 minutes
                "notebooklm": {
                    "auto_generate": False,  # Manual NotebookLM process
                    "export_format": "markdown"
                }
            }
            
            # Save default config
            os.makedirs(os.path.dirname(config_path) or ".", exist_ok=True)
            with open(config_path, 'w') as f:
                json.dump(default_config, f, indent=2)
            
            return default_config
    
    def generate_weekly_digest(self) -> Dict[str, Any]:
        """
        Step 1: Generate weekly digest from aggregated content
        """
        logger.info("=" * 50)
        logger.info("STEP 1: Generating Weekly Digest")
        logger.info("=" * 50)
        
        # Run weekly aggregator
        digest = self.weekly_aggregator.generate_digest()
        
        # Save digest metadata
        digest_file = self.output_path / f"digest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(digest_file, 'w') as f:
            json.dump(digest, f, indent=2, default=str)
        
        logger.info(f"Weekly digest generated: {digest_file}")
        logger.info(f"- Theme: {digest.get('theme', 'Weekly Update')}")
        logger.info(f"- Stories: {len(digest.get('top_stories', []))}")
        logger.info(f"- Word count: {digest.get('word_count', 0)}")
        
        return digest
    
    def generate_audio(self, digest: Dict[str, Any]) -> Optional[str]:
        """
        Step 2: Generate audio using configured method (Podcastfy or NotebookLM)
        """
        logger.info("=" * 50)
        logger.info(f"STEP 2: Generating Audio ({self.audio_method})")
        logger.info("=" * 50)
        
        if self.audio_method == "podcastfy":
            return self._generate_with_podcastfy(digest)
        else:
            return self._generate_with_notebooklm(digest)
    
    def _generate_with_podcastfy(self, digest: Dict[str, Any]) -> str:
        """Generate audio using Podcastfy (automated)"""
        logger.info("🤖 Using Podcastfy for automated audio generation")
        
        try:
            # Generate audio directly from digest
            audio_file = self.podcastfy_generator.generate_podcast_from_digest(digest)
            logger.info(f"✅ Audio generated successfully: {audio_file}")
            return audio_file
            
        except Exception as e:
            logger.error(f"Podcastfy generation failed: {e}")
            
            # Fallback to NotebookLM if available
            if NOTEBOOKLM_AVAILABLE:
                logger.info("Falling back to NotebookLM manual process...")
                return self._generate_with_notebooklm(digest)
            else:
                raise
    
    def _generate_with_notebooklm(self, digest: Dict[str, Any]) -> Optional[str]:
        """Generate audio using NotebookLM (manual process)"""
        logger.info("📝 Using NotebookLM export (manual process required)")
        
        # Export for NotebookLM
        export_file = self._export_for_notebooklm(digest)
        
        # Wait for manual audio creation
        audio_file = self.wait_for_audio()
        
        return audio_file
    
    def _export_for_notebooklm(self, digest: Dict[str, Any]) -> str:
        """
        Export digest to NotebookLM format
        """
        logger.info("Exporting digest for NotebookLM...")
        
        # Export to markdown for NotebookLM
        export_file = self.notebooklm_exporter.export_digest(
            digest=digest,
            output_path=self.output_path,
            format="markdown"
        )
        
        # Also create a structured version for NotebookLM sources
        sources_file = self.output_path / f"notebooklm_sources_{datetime.now().strftime('%Y%m%d')}.json"
        sources_data = {
            "title": f"Regen Network Weekly Digest - Week {datetime.now().isocalendar()[1]}",
            "sources": [
                {
                    "type": "markdown",
                    "path": export_file,
                    "title": "Weekly Digest Content"
                }
            ],
            "instructions": """
            Create a 20-minute conversational podcast discussing this week's developments in the Regen Network ecosystem.
            
            Format:
            - Two hosts having a natural conversation
            - Start with brief introduction and overview
            - Discuss 3-4 main topics in depth
            - Include explanations for technical concepts
            - End with forward-looking discussion
            
            Tone:
            - Informative but accessible
            - Enthusiastic about regenerative solutions
            - Balanced perspective on challenges and opportunities
            """
        }
        
        with open(sources_file, 'w') as f:
            json.dump(sources_data, f, indent=2)
        
        logger.info(f"NotebookLM export complete:")
        logger.info(f"- Markdown: {export_file}")
        logger.info(f"- Sources: {sources_file}")
        
        return export_file
    
    def wait_for_audio(self, timeout: Optional[int] = None) -> Optional[str]:
        """
        Step 3: Wait for NotebookLM audio file
        
        This monitors a directory for the audio file that should be
        manually generated and downloaded from NotebookLM
        """
        logger.info("=" * 50)
        logger.info("STEP 3: Waiting for NotebookLM Audio")
        logger.info("=" * 50)
        
        logger.info("\n" + "!" * 50)
        logger.info("MANUAL STEP REQUIRED:")
        logger.info("1. Open NotebookLM (notebooklm.google.com)")
        logger.info("2. Create new notebook or use existing 'Regen Weekly' notebook")
        logger.info(f"3. Upload the markdown file from: {self.output_path}")
        logger.info("4. Click 'Generate Audio Overview'")
        logger.info("5. Download the audio file when complete")
        logger.info(f"6. Save it to: {self.watch_path}")
        logger.info("!" * 50 + "\n")
        
        timeout = timeout or self.config.get("watch_timeout", 3600)
        start_time = time.time()
        
        logger.info(f"Watching directory: {self.watch_path}")
        logger.info(f"Timeout: {timeout} seconds")
        
        # Look for audio files
        audio_extensions = ['.mp3', '.m4a', '.wav', '.aac']
        
        while time.time() - start_time < timeout:
            # Check for new audio files
            for ext in audio_extensions:
                audio_files = list(self.watch_path.glob(f"*{ext}"))
                
                # Filter files created after we started watching
                new_files = [
                    f for f in audio_files 
                    if f.stat().st_mtime > start_time
                ]
                
                if new_files:
                    audio_file = new_files[0]  # Take the first new file
                    logger.info(f"✅ Audio file detected: {audio_file}")
                    
                    # Validate duration
                    if self._validate_audio_duration(audio_file):
                        return str(audio_file)
                    else:
                        logger.warning(f"Audio file duration outside acceptable range")
            
            # Wait before checking again
            time.sleep(10)
            
            # Show progress
            elapsed = int(time.time() - start_time)
            if elapsed % 60 == 0:  # Every minute
                remaining = timeout - elapsed
                logger.info(f"Still waiting... ({remaining} seconds remaining)")
        
        logger.error(f"Timeout reached. No audio file found in {timeout} seconds")
        return None
    
    def _validate_audio_duration(self, audio_file: Path) -> bool:
        """Validate that audio duration is within acceptable range"""
        try:
            duration = self.audio_pipeline._get_audio_duration(str(audio_file))
            
            # Parse duration string (HH:MM:SS)
            parts = duration.split(':')
            total_seconds = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            
            min_duration = self.config.get("min_audio_duration", 900)  # 15 min
            max_duration = self.config.get("max_audio_duration", 1800)  # 30 min
            
            if min_duration <= total_seconds <= max_duration:
                logger.info(f"Audio duration valid: {duration} ({total_seconds} seconds)")
                return True
            else:
                logger.warning(f"Audio duration out of range: {duration}")
                logger.warning(f"Expected between {min_duration} and {max_duration} seconds")
                return False
                
        except Exception as e:
            logger.error(f"Could not validate audio duration: {e}")
            return True  # Accept if we can't validate
    
    def publish_podcast(self, audio_file: str, digest: Dict[str, Any]) -> Dict[str, Any]:
        """
        Step 4: Publish podcast episode
        """
        logger.info("=" * 50)
        logger.info("STEP 4: Publishing Podcast Episode")
        logger.info("=" * 50)
        
        # Publish episode
        episode = self.podcast_publisher.publish_episode_from_weekly_digest(
            audio_file=audio_file,
            digest_metadata=digest
        )
        
        # Get feed URL
        feed_file = self.podcast_publisher.feed_path / "feed.xml"
        
        result = {
            "episode_number": episode.episode_number,
            "title": episode.title,
            "audio_url": episode.audio_url,
            "feed_file": str(feed_file),
            "publication_date": episode.publication_date.isoformat()
        }
        
        logger.info("✅ Podcast published successfully!")
        logger.info(f"- Episode: {episode.title}")
        logger.info(f"- Number: {episode.episode_number}")
        logger.info(f"- Feed: {feed_file}")
        
        return result
    
    def run_full_pipeline(self, auto_publish: bool = False) -> Dict[str, Any]:
        """
        Run the complete podcast generation pipeline
        
        Args:
            auto_publish: If True, publishes immediately when audio is ready
        
        Returns:
            Pipeline results
        """
        logger.info("=" * 50)
        logger.info("PODCAST GENERATION PIPELINE")
        logger.info("=" * 50)
        
        results = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "status": "in_progress"
        }
        
        try:
            # Step 1: Generate weekly digest
            digest = self.generate_weekly_digest()
            results["digest"] = {
                "generated": True,
                "theme": digest.get("theme"),
                "word_count": digest.get("word_count", 0)
            }
            
            # Step 2: Generate audio (Podcastfy or NotebookLM)
            audio_file = self.generate_audio(digest)
            results["audio_method"] = self.audio_method
            
            if audio_file:
                results["audio_file"] = audio_file
                
                # Step 4: Publish if auto_publish or ask for confirmation
                if auto_publish or self._confirm_publish():
                    publish_result = self.publish_podcast(audio_file, digest)
                    results["published"] = publish_result
                    results["status"] = "published"
                else:
                    results["status"] = "ready_to_publish"
                    logger.info("Podcast ready but not published (manual confirmation required)")
            else:
                results["status"] = "audio_timeout"
                logger.error("Pipeline stopped: No audio file received")
        
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
            results["status"] = "error"
            results["error"] = str(e)
        
        # Save results
        results["completed_at"] = datetime.now(timezone.utc).isoformat()
        results_file = self.output_path / f"pipeline_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"\nPipeline results saved to: {results_file}")
        return results
    
    def _confirm_publish(self) -> bool:
        """Ask for confirmation before publishing"""
        response = input("\n🎙️ Ready to publish podcast. Continue? (yes/no): ")
        return response.lower() in ['yes', 'y']


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Podcast Generation Pipeline")
    parser.add_argument(
        "--auto-publish",
        action="store_true",
        help="Automatically publish when audio is ready"
    )
    parser.add_argument(
        "--skip-digest",
        action="store_true",
        help="Skip digest generation (use existing)"
    )
    parser.add_argument(
        "--audio-file",
        help="Path to existing audio file (skip waiting)"
    )
    parser.add_argument(
        "--config",
        default="config/podcast_integration.json",
        help="Configuration file path"
    )
    
    args = parser.parse_args()
    
    # Initialize integration
    integration = PodcastIntegration(config_path=args.config)
    
    if args.audio_file and os.path.exists(args.audio_file):
        # Shortcut: Just publish existing audio
        logger.info("Using existing audio file")
        
        # Load latest digest or create minimal one
        digest_files = list(integration.output_path.glob("digest_*.json"))
        if digest_files:
            latest_digest = max(digest_files, key=lambda p: p.stat().st_mtime)
            with open(latest_digest, 'r') as f:
                digest = json.load(f)
        else:
            digest = {
                "theme": "Weekly Update",
                "week_ending": datetime.now(timezone.utc).isoformat(),
                "executive_summary": "Weekly digest of Regen Network activities"
            }
        
        # Publish
        result = integration.publish_podcast(args.audio_file, digest)
        print(f"\n✅ Published: {result}")
    else:
        # Run full pipeline
        results = integration.run_full_pipeline(auto_publish=args.auto_publish)
        
        # Print summary
        print("\n" + "=" * 50)
        print("PIPELINE COMPLETE")
        print("=" * 50)
        print(f"Status: {results['status']}")
        
        if results.get('published'):
            print(f"Episode: {results['published']['title']}")
            print(f"Feed: {results['published']['feed_file']}")


if __name__ == "__main__":
    main()