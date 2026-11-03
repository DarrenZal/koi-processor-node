#!/usr/bin/env python3
"""
Enhanced Audio Pipeline for Regen Network Weekly Digest (Session 13)

Builds on Session 9 to add:
- Audio file retrieval system for NotebookLM
- 20-minute duration validation
- Audio storage and versioning
- Metadata tagging
- Multiple quality versions
"""

import json
import os
import sys
import logging
import hashlib
import subprocess
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple, List
from pathlib import Path
import shutil

# Audio processing libraries
try:
    from mutagen.mp3 import MP3
    from mutagen.id3 import ID3, TIT2, TPE1, TALB, TDRC, APIC, COMM, TCON, TPOS
    MUTAGEN_AVAILABLE = True
except ImportError:
    MUTAGEN_AVAILABLE = False
    print("Warning: mutagen not installed. Audio metadata features disabled.")
    print("Install with: pip install mutagen")

# Import our modules
PODCASTFY_AVAILABLE = False
try:
    from weekly_aggregator import WeeklyAggregator
    from notebooklm_exporter import NotebookLMExporter
except ImportError as e:
    print(f"Warning: Could not import module: {e}")
    print("Some features may be unavailable.")
    
try:
    from podcastfy_generator import PodcastfyGenerator
    PODCASTFY_AVAILABLE = True
except ImportError:
    pass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EnhancedAudioPipeline:
    """
    Enhanced pipeline for generating and managing audio content from weekly digests.
    
    New features (Session 13):
    - Audio file retrieval and monitoring
    - Duration validation (20-minute target)
    - Storage versioning system
    - Comprehensive metadata tagging
    """
    
    def __init__(self, config_path: str = "config/audio_pipeline.json"):
        """Initialize the enhanced audio pipeline"""
        self.config = self._load_config(config_path)
        self.weekly_aggregator = None
        self.notebooklm_exporter = NotebookLMExporter() if 'NotebookLMExporter' in globals() else None
        self.podcastfy_generator = PodcastfyGenerator() if PODCASTFY_AVAILABLE else None
        
        # Audio storage configuration
        self.storage_config = self.config.get("storage", {
            "versions_to_keep": 5,
            "archive_old_versions": True,
            "compression_enabled": True
        })
        
        # Initialize storage directories
        self._initialize_storage()
    
    def _load_config(self, config_path: str) -> Dict:
        """Load pipeline configuration"""
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
        else:
            # Enhanced default configuration
            return {
                "pipeline": {
                    "generate_digest": True,
                    "export_notebooklm": True,
                    "generate_podcast": True,
                    "validate_duration": True,
                    "create_versions": True,
                    "archive_outputs": True
                },
                "digest": {
                    "days_back": 7,
                    "config_file": "config/weekly_aggregator.json"
                },
                "audio": {
                    "backend": "podcastfy",  # or "notebooklm_manual"
                    "target_duration_minutes": 20,
                    "min_duration_minutes": 16,  # 80% of target
                    "max_duration_minutes": 24,  # 120% of target
                    "preferred_format": "mp3",
                    "preferred_bitrate": "192k",
                    "metadata_enabled": True
                },
                "storage": {
                    "versions_to_keep": 5,
                    "archive_old_versions": True,
                    "compression_enabled": True
                },
                "output": {
                    "digest_dir": "output/weekly",
                    "notebooklm_dir": "output/notebooklm",
                    "podcast_dir": "output/podcasts",
                    "archive_dir": "output/archive",
                    "versions_dir": "output/versions",
                    "watch_dir": "output/notebooklm_uploads"  # For manual uploads
                },
                "distribution": {
                    "podcast_feed_dir": "/var/www/podcasts",
                    "enable_auto_publish": False,
                    "rss_feed_file": "regen_weekly.xml"
                },
                "monitoring": {
                    "watch_interval_seconds": 30,
                    "max_wait_minutes": 30,
                    "notification_enabled": False
                }
            }
    
    def _initialize_storage(self):
        """Initialize storage directories"""
        directories = [
            self.config["output"].get("digest_dir", "output/weekly"),
            self.config["output"].get("notebooklm_dir", "output/notebooklm"),
            self.config["output"].get("podcast_dir", "output/podcasts"),
            self.config["output"].get("archive_dir", "output/archive"),
            self.config["output"].get("versions_dir", "output/audio_versions")
        ]
        # Add watch_dir if specified
        if "watch_dir" in self.config["output"]:
            directories.append(self.config["output"]["watch_dir"])
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
        
        logger.info("Storage directories initialized")
    
    def validate_audio_duration(self, audio_file: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Validate that the audio file meets the 20-minute duration requirement.
        
        Args:
            audio_file: Path to the audio file
            
        Returns:
            Tuple of (meets_requirements, validation_info)
        """
        validation_info = {
            "file": audio_file,
            "exists": os.path.exists(audio_file),
            "timestamp": datetime.now().isoformat()
        }
        
        if not validation_info["exists"]:
            return False, {**validation_info, "error": "File not found"}
        
        # Get file info
        validation_info["file_size_mb"] = os.path.getsize(audio_file) / (1024 * 1024)
        
        if MUTAGEN_AVAILABLE:
            try:
                audio = MP3(audio_file)
                duration_seconds = audio.info.length
                duration_minutes = duration_seconds / 60
                
                validation_info.update({
                    "duration_seconds": duration_seconds,
                    "duration_minutes": duration_minutes,
                    "duration_formatted": f"{int(duration_minutes)}:{int(duration_seconds % 60):02d}",
                    "bitrate": audio.info.bitrate if hasattr(audio.info, 'bitrate') else None,
                    "sample_rate": audio.info.sample_rate if hasattr(audio.info, 'sample_rate') else None,
                    "channels": audio.info.channels if hasattr(audio.info, 'channels') else None
                })
                
                # Check against requirements
                target = self.config["audio"]["target_duration_minutes"]
                min_duration = self.config["audio"]["min_duration_minutes"]
                max_duration = self.config["audio"]["max_duration_minutes"]
                
                validation_info["target_minutes"] = target
                validation_info["acceptable_range"] = f"{min_duration}-{max_duration} minutes"
                
                meets_requirements = min_duration <= duration_minutes <= max_duration
                validation_info["meets_requirements"] = meets_requirements
                
                if meets_requirements:
                    validation_info["status"] = "PASSED"
                    logger.info(f"✓ Audio duration validated: {duration_minutes:.1f} minutes")
                else:
                    validation_info["status"] = "FAILED"
                    if duration_minutes < min_duration:
                        validation_info["issue"] = f"Too short ({duration_minutes:.1f} < {min_duration})"
                    else:
                        validation_info["issue"] = f"Too long ({duration_minutes:.1f} > {max_duration})"
                    logger.warning(f"✗ Audio duration outside range: {validation_info['issue']}")
                
                return meets_requirements, validation_info
                
            except Exception as e:
                logger.error(f"Error validating audio with mutagen: {e}")
                validation_info["error"] = str(e)
                
        # Fallback: Use ffprobe if available
        try:
            cmd = [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                audio_file
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                duration_seconds = float(result.stdout.strip())
                duration_minutes = duration_seconds / 60
                
                validation_info["duration_seconds"] = duration_seconds
                validation_info["duration_minutes"] = duration_minutes
                validation_info["method"] = "ffprobe"
                
                target = self.config["audio"]["target_duration_minutes"]
                min_duration = self.config["audio"]["min_duration_minutes"]
                max_duration = self.config["audio"]["max_duration_minutes"]
                
                meets_requirements = min_duration <= duration_minutes <= max_duration
                validation_info["meets_requirements"] = meets_requirements
                
                return meets_requirements, validation_info
                
        except Exception as e:
            logger.error(f"Error validating audio with ffprobe: {e}")
            validation_info["error"] = f"Could not determine duration: {e}"
            
        return False, validation_info
    
    def watch_for_notebooklm_audio(
        self,
        export_dir: str,
        max_wait_minutes: Optional[int] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Watch for NotebookLM audio file to be uploaded.
        
        Args:
            export_dir: Directory where NotebookLM sources were exported
            max_wait_minutes: Maximum time to wait for audio
            
        Returns:
            Tuple of (found, audio_file_path)
        """
        watch_dir = self.config["output"]["watch_dir"]
        max_wait = max_wait_minutes or self.config["monitoring"]["max_wait_minutes"]
        check_interval = self.config["monitoring"]["watch_interval_seconds"]
        
        logger.info(f"Watching for NotebookLM audio in: {watch_dir}")
        logger.info(f"Will check every {check_interval} seconds for up to {max_wait} minutes")
        
        # Create instruction file
        instructions = f"""
        NOTEBOOKLM AUDIO UPLOAD INSTRUCTIONS:
        
        1. Generate audio in NotebookLM using sources from: {export_dir}
        2. Download the generated audio file
        3. Place it in this directory: {watch_dir}
        4. Name it with today's date: regen_weekly_{datetime.now().strftime('%Y-%m-%d')}.mp3
        
        The system is watching this directory and will automatically:
        - Validate the duration
        - Add metadata
        - Create versions
        - Move to podcast directory
        """
        
        instructions_file = os.path.join(watch_dir, "UPLOAD_AUDIO_HERE.txt")
        with open(instructions_file, 'w') as f:
            f.write(instructions)
        
        # Watch for audio file
        start_time = time.time()
        max_wait_seconds = max_wait * 60
        
        audio_patterns = ["*.mp3", "*.m4a", "*.wav"]
        
        while (time.time() - start_time) < max_wait_seconds:
            # Check for audio files
            for pattern in audio_patterns:
                for audio_file in Path(watch_dir).glob(pattern):
                    # Skip instruction file
                    if audio_file.name.startswith("UPLOAD"):
                        continue
                    
                    logger.info(f"Found audio file: {audio_file}")
                    
                    # Validate duration
                    valid, info = self.validate_audio_duration(str(audio_file))
                    
                    if valid:
                        # Move to podcast directory
                        podcast_dir = self.config["output"]["podcast_dir"]
                        date_str = datetime.now().strftime('%Y-%m-%d')
                        dest_path = os.path.join(
                            podcast_dir,
                            f"regen_weekly_{date_str}{audio_file.suffix}"
                        )
                        
                        shutil.move(str(audio_file), dest_path)
                        logger.info(f"Audio moved to: {dest_path}")
                        
                        return True, dest_path
                    else:
                        logger.warning(f"Audio validation failed: {info}")
                        # Keep the file for manual review
                        failed_dir = os.path.join(watch_dir, "failed_validation")
                        os.makedirs(failed_dir, exist_ok=True)
                        shutil.move(str(audio_file), os.path.join(failed_dir, audio_file.name))
            
            # Wait before next check
            time.sleep(check_interval)
            elapsed = int((time.time() - start_time) / 60)
            if elapsed % 5 == 0:  # Log every 5 minutes
                logger.info(f"Still watching... ({elapsed}/{max_wait} minutes)")
        
        logger.warning(f"Timeout: No valid audio file found after {max_wait} minutes")
        return False, None
    
    def add_podcast_metadata(self, audio_file: str, digest_data: Dict[str, Any]) -> bool:
        """
        Add comprehensive metadata tags to the podcast audio file.
        
        Args:
            audio_file: Path to the audio file
            digest_data: Weekly digest data
            
        Returns:
            Success status
        """
        if not MUTAGEN_AVAILABLE:
            logger.warning("Mutagen not available, skipping metadata")
            return False
        
        try:
            audio = MP3(audio_file, ID3=ID3)
            
            # Add ID3 tags if not present
            if audio.tags is None:
                audio.add_tags()
            
            # Episode information
            week_end = digest_data.get("week_end", datetime.now().isoformat())
            episode_date = datetime.fromisoformat(week_end.replace('Z', '+00:00'))
            
            # Calculate episode number (weeks since start)
            start_date = datetime(2025, 1, 1, tzinfo=episode_date.tzinfo)
            episode_number = ((episode_date - start_date).days // 7) + 1
            
            # Title
            audio.tags.add(TIT2(
                encoding=3,
                text=f"Regen Weekly #{episode_number} - {episode_date.strftime('%B %d, %Y')}"
            ))
            
            # Artist/Podcast
            audio.tags.add(TPE1(encoding=3, text="Regen Network"))
            
            # Album/Show
            audio.tags.add(TALB(encoding=3, text="Regen Weekly Digest"))
            
            # Year
            audio.tags.add(TDRC(encoding=3, text=str(episode_date.year)))
            
            # Genre
            audio.tags.add(TCON(encoding=3, text="Podcast"))
            
            # Track/Episode number
            audio.tags.add(TPOS(encoding=3, text=str(episode_number)))
            
            # Description from brief
            brief = digest_data.get("brief", "")
            if brief:
                # Extract executive summary
                lines = brief.split('\n')
                summary = ""
                for line in lines:
                    if "Executive Summary" in line:
                        # Get next few lines
                        idx = lines.index(line)
                        summary = ' '.join(lines[idx+1:idx+4])
                        break
                
                if not summary:
                    summary = brief[:500]  # First 500 chars
                
                audio.tags.add(COMM(
                    encoding=3,
                    lang='eng',
                    desc='Description',
                    text=summary
                ))
            
            # Add topics as comments
            topics = digest_data.get("themes", [])
            if topics:
                audio.tags.add(COMM(
                    encoding=3,
                    lang='eng',
                    desc='Topics',
                    text=', '.join(topics[:5])  # Top 5 topics
                ))
            
            # Add cover art if available
            cover_paths = [
                "assets/podcast_cover.jpg",
                "assets/podcast_cover.png",
                "output/assets/podcast_cover.jpg"
            ]
            
            for cover_path in cover_paths:
                if os.path.exists(cover_path):
                    with open(cover_path, 'rb') as f:
                        audio.tags.add(
                            APIC(
                                encoding=3,
                                mime='image/jpeg' if cover_path.endswith('.jpg') else 'image/png',
                                type=3,  # Cover image
                                desc='Cover',
                                data=f.read()
                            )
                        )
                    logger.info(f"Added cover art from {cover_path}")
                    break
            
            # Save with metadata
            audio.save()
            logger.info(f"✓ Metadata added to {audio_file}")
            
            # Log metadata summary
            logger.info(f"  Episode: #{episode_number}")
            logger.info(f"  Date: {episode_date.strftime('%B %d, %Y')}")
            logger.info(f"  Topics: {', '.join(topics[:3])}" if topics else "  Topics: None")
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding metadata: {e}")
            return False
    
    def create_audio_versions(self, audio_file: str) -> Dict[str, str]:
        """
        Create different versions of the audio file with versioning.
        
        Args:
            audio_file: Path to the original audio file
            
        Returns:
            Dictionary of version_name -> file_path
        """
        versions = {}
        versions_dir = self.config["output"]["versions_dir"]
        base_name = os.path.splitext(os.path.basename(audio_file))[0]
        date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create version subdirectory
        version_subdir = os.path.join(versions_dir, f"{base_name}_{date_str}")
        os.makedirs(version_subdir, exist_ok=True)
        
        try:
            # Copy original as high quality
            high_path = os.path.join(version_subdir, f"{base_name}_high.mp3")
            shutil.copy2(audio_file, high_path)
            versions["high"] = high_path
            logger.info(f"Created high quality version: {high_path}")
            
            # Check if ffmpeg is available
            ffmpeg_available = subprocess.run(
                ["which", "ffmpeg"],
                capture_output=True
            ).returncode == 0
            
            if ffmpeg_available:
                # Medium quality (128kbps)
                medium_file = os.path.join(version_subdir, f"{base_name}_medium.mp3")
                cmd = [
                    "ffmpeg", "-i", audio_file,
                    "-codec:a", "libmp3lame",
                    "-b:a", "128k",
                    "-ar", "44100",  # Standard sample rate
                    "-ac", "2",  # Stereo
                    "-y", medium_file
                ]
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    versions["medium"] = medium_file
                    logger.info(f"Created medium quality version (128kbps)")
                
                # Low quality (64kbps) for preview/mobile
                low_file = os.path.join(version_subdir, f"{base_name}_low.mp3")
                cmd = [
                    "ffmpeg", "-i", audio_file,
                    "-codec:a", "libmp3lame",
                    "-b:a", "64k",
                    "-ar", "22050",  # Lower sample rate
                    "-ac", "1",  # Mono
                    "-y", low_file
                ]
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    versions["low"] = low_file
                    logger.info(f"Created low quality version (64kbps)")
                
                # Create 60-second preview
                preview_file = os.path.join(version_subdir, f"{base_name}_preview.mp3")
                cmd = [
                    "ffmpeg", "-i", audio_file,
                    "-t", "60",  # 60 seconds
                    "-codec:a", "libmp3lame",
                    "-b:a", "128k",
                    "-y", preview_file
                ]
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    versions["preview"] = preview_file
                    logger.info(f"Created 60-second preview")
                
                # Create chapter markers file (if digest has sections)
                # This would be used by podcast apps that support chapters
                
            else:
                logger.warning("ffmpeg not available, only high quality version created")
            
            # Create version manifest
            manifest = {
                "created_at": datetime.now().isoformat(),
                "original_file": audio_file,
                "versions": {}
            }
            
            for version_name, version_path in versions.items():
                if os.path.exists(version_path):
                    manifest["versions"][version_name] = {
                        "path": version_path,
                        "size_mb": os.path.getsize(version_path) / (1024 * 1024),
                        "relative_path": os.path.relpath(version_path, versions_dir)
                    }
            
            manifest_path = os.path.join(version_subdir, "manifest.json")
            with open(manifest_path, 'w') as f:
                json.dump(manifest, f, indent=2)
            
            logger.info(f"Version manifest saved to {manifest_path}")
            
            # Clean up old versions if needed
            self._cleanup_old_versions(base_name)
            
        except Exception as e:
            logger.error(f"Error creating audio versions: {e}")
        
        return versions
    
    def _cleanup_old_versions(self, base_name: str):
        """
        Clean up old versions keeping only the configured number.
        
        Args:
            base_name: Base name of the audio file
        """
        versions_dir = self.config["output"]["versions_dir"]
        versions_to_keep = self.storage_config["versions_to_keep"]
        
        # Find all version directories for this base name
        version_dirs = []
        for item in os.listdir(versions_dir):
            if item.startswith(base_name) and os.path.isdir(os.path.join(versions_dir, item)):
                version_dirs.append(item)
        
        # Sort by creation time (newest first)
        version_dirs.sort(reverse=True)
        
        # Archive or delete old versions
        if len(version_dirs) > versions_to_keep:
            old_versions = version_dirs[versions_to_keep:]
            
            for old_version in old_versions:
                old_path = os.path.join(versions_dir, old_version)
                
                if self.storage_config["archive_old_versions"]:
                    # Move to archive
                    archive_dir = os.path.join(self.config["output"]["archive_dir"], "old_versions")
                    os.makedirs(archive_dir, exist_ok=True)
                    
                    archive_path = os.path.join(archive_dir, old_version)
                    shutil.move(old_path, archive_path)
                    logger.info(f"Archived old version: {old_version}")
                else:
                    # Delete old version
                    shutil.rmtree(old_path)
                    logger.info(f"Deleted old version: {old_version}")
    
    def generate_storage_report(self) -> Dict[str, Any]:
        """
        Generate a report on audio storage usage and versions.
        
        Returns:
            Storage report dictionary
        """
        report = {
            "generated_at": datetime.now().isoformat(),
            "directories": {},
            "total_size_mb": 0,
            "file_count": 0,
            "versions": {}
        }
        
        # Check each output directory
        for dir_name, dir_path in self.config["output"].items():
            if os.path.exists(dir_path):
                size = 0
                count = 0
                
                for root, dirs, files in os.walk(dir_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        size += os.path.getsize(file_path)
                        count += 1
                
                size_mb = size / (1024 * 1024)
                report["directories"][dir_name] = {
                    "path": dir_path,
                    "size_mb": round(size_mb, 2),
                    "file_count": count
                }
                report["total_size_mb"] += size_mb
                report["file_count"] += count
        
        # Count versions
        versions_dir = self.config["output"]["versions_dir"]
        if os.path.exists(versions_dir):
            version_counts = {}
            for item in os.listdir(versions_dir):
                if os.path.isdir(os.path.join(versions_dir, item)):
                    base = item.rsplit('_', 2)[0]  # Remove timestamp
                    version_counts[base] = version_counts.get(base, 0) + 1
            
            report["versions"] = version_counts
        
        report["total_size_mb"] = round(report["total_size_mb"], 2)
        
        return report
    
    def process_notebooklm_audio(
        self,
        audio_file: str,
        digest_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Process audio file from NotebookLM.
        
        Args:
            audio_file: Path to the audio file
            digest_data: Weekly digest data
            
        Returns:
            Processing results
        """
        results = {
            "success": False,
            "audio_file": audio_file,
            "validation": None,
            "metadata_added": False,
            "versions_created": [],
            "errors": []
        }
        
        try:
            # Step 1: Validate duration
            valid, validation_info = self.validate_audio_duration(audio_file)
            results["validation"] = validation_info
            
            if not valid:
                results["errors"].append(f"Duration validation failed: {validation_info}")
                return results
            
            # Step 2: Add metadata
            if self.config["audio"]["metadata_enabled"]:
                metadata_added = self.add_podcast_metadata(audio_file, digest_data)
                results["metadata_added"] = metadata_added
                
                if not metadata_added:
                    results["errors"].append("Failed to add metadata")
            
            # Step 3: Create versions
            if self.config["pipeline"]["create_versions"]:
                versions = self.create_audio_versions(audio_file)
                results["versions_created"] = list(versions.keys())
                
                if not versions:
                    results["errors"].append("Failed to create versions")
            
            results["success"] = True
            logger.info(f"✓ NotebookLM audio processed successfully")
            
        except Exception as e:
            results["errors"].append(str(e))
            logger.error(f"Error processing NotebookLM audio: {e}")
        
        return results


def main():
    """
    Test the enhanced audio pipeline.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Enhanced Audio Pipeline for Regen Weekly")
    parser.add_argument(
        "--action",
        choices=["validate", "watch", "process", "versions", "report"],
        default="validate",
        help="Action to perform"
    )
    parser.add_argument(
        "--audio",
        help="Path to audio file"
    )
    parser.add_argument(
        "--digest",
        help="Path to digest JSON file"
    )
    parser.add_argument(
        "--export-dir",
        help="NotebookLM export directory"
    )
    parser.add_argument(
        "--wait",
        type=int,
        default=30,
        help="Minutes to wait for audio (default: 30)"
    )
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = EnhancedAudioPipeline()
    
    if args.action == "validate":
        if not args.audio:
            print("Error: --audio required for validation")
            sys.exit(1)
        
        valid, info = pipeline.validate_audio_duration(args.audio)
        print(json.dumps(info, indent=2))
        
        if valid:
            print(f"\n✓ Audio meets requirements")
        else:
            print(f"\n✗ Audio validation failed")
            sys.exit(1)
    
    elif args.action == "watch":
        export_dir = args.export_dir or "output/notebooklm"
        found, audio_file = pipeline.watch_for_notebooklm_audio(export_dir, args.wait)
        
        if found:
            print(f"\n✓ Audio found: {audio_file}")
        else:
            print(f"\n✗ No audio found after {args.wait} minutes")
            sys.exit(1)
    
    elif args.action == "process":
        if not args.audio:
            print("Error: --audio required for processing")
            sys.exit(1)
        
        # Load digest data if provided
        digest_data = {}
        if args.digest and os.path.exists(args.digest):
            with open(args.digest, 'r') as f:
                digest_data = json.load(f)
        else:
            # Use test data
            digest_data = {
                "week_end": datetime.now().isoformat(),
                "brief": "Test weekly digest brief",
                "themes": ["regenerative agriculture", "carbon credits", "governance"]
            }
        
        results = pipeline.process_notebooklm_audio(args.audio, digest_data)
        print(json.dumps(results, indent=2))
        
        if results["success"]:
            print(f"\n✓ Audio processed successfully")
        else:
            print(f"\n✗ Processing failed: {results['errors']}")
            sys.exit(1)
    
    elif args.action == "versions":
        if not args.audio:
            print("Error: --audio required for version creation")
            sys.exit(1)
        
        versions = pipeline.create_audio_versions(args.audio)
        print("\nCreated versions:")
        for name, path in versions.items():
            size_mb = os.path.getsize(path) / (1024 * 1024) if os.path.exists(path) else 0
            print(f"  {name}: {path} ({size_mb:.1f} MB)")
    
    elif args.action == "report":
        report = pipeline.generate_storage_report()
        print(json.dumps(report, indent=2))
        print(f"\nTotal storage: {report['total_size_mb']:.1f} MB")
        print(f"Total files: {report['file_count']}")


if __name__ == "__main__":
    main()