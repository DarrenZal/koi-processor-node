#!/usr/bin/env python3
"""
Simple Podcast Generator using OpenAI TTS
Generates audio podcast from weekly digest using OpenAI's text-to-speech API
"""

import os
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    logger.warning("OpenAI not installed. Install with: pip install openai")

class SimplePodcastGenerator:
    """Generate podcast using OpenAI TTS directly"""

    def __init__(self):
        if not OPENAI_AVAILABLE:
            raise ImportError("OpenAI library required. Install with: pip install openai")

        api_key = os.environ.get('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY not found in environment")

        self.client = OpenAI(api_key=api_key)
        self.output_dir = Path("podcast_audio")
        self.output_dir.mkdir(exist_ok=True)

    def generate_from_digest(self, digest_path: str) -> str:
        """Generate podcast from digest JSON file"""

        # Load digest
        with open(digest_path, 'r') as f:
            digest = json.load(f)

        logger.info(f"Loaded digest with {len(digest.get('top_stories', []))} stories")

        # Create script from digest
        script = self._create_script(digest)
        logger.info(f"Created script: {len(script)} characters")

        # Generate audio
        output_file = self._generate_audio(script, digest)
        logger.info(f"Generated podcast: {output_file}")

        return output_file

    def _create_script(self, digest: Dict[str, Any]) -> str:
        """Create podcast script from digest"""

        # First check if we have a pre-generated podcast script
        if digest.get('podcast_script'):
            # Use the existing podcast script (designed for ~20 minutes)
            script = digest['podcast_script']

            # Clean up markdown formatting for TTS
            script = script.replace('#', '').replace('*', '').replace('_', '')
            script = script.replace('**', '')  # Remove bold
            script = script.replace('---', 'Moving on. ')  # Replace horizontal rules
            script = script.replace('- ', '. ')  # Replace bullet points
            script = script.replace('•', '. ')  # Replace bullet points

            # Expand the script with better pacing for 20 minutes
            lines = []

            # Opening with music cue
            lines.append("Welcome to the Regen Network Weekly Digest Podcast.")
            lines.append("Your comprehensive source for regenerative finance and ecological blockchain innovation.")
            lines.append("")

            # Add the cleaned script
            lines.append(script)
            lines.append("")

            # If we have brief_content, add it for more detail
            if digest.get('brief_content'):
                lines.append("Now, let's explore these developments in greater detail.")
                brief = digest['brief_content']
                # Clean up markdown
                brief = brief.replace('#', '').replace('*', '').replace('_', '')
                brief = brief.replace('**', '')
                brief = brief.replace('---', 'Next topic. ')
                brief = brief.replace('- ', '. ')
                lines.append(brief)
                lines.append("")

            expanded_script = "\n".join(lines)
            logger.info(f"Using expanded podcast script: {len(expanded_script)} characters")
            return expanded_script

        # Otherwise use the full content approach
        lines = []

        # Introduction
        lines.append("Welcome to the Regen Network Weekly Digest podcast.")
        week_start = digest.get('week_start', 'recent')
        week_end = digest.get('week_end', 'today')
        lines.append(f"This week covers {week_start} through {week_end}.")
        lines.append("")

        # Executive summary (full, not truncated)
        if digest.get('executive_summary'):
            lines.append("Let's begin with this week's executive summary.")
            lines.append(digest['executive_summary'])
            lines.append("Now, let's dive deeper into this week's developments.")
            lines.append("")

        # Brief content (full weekly brief)
        if digest.get('brief_content'):
            lines.append("Here's our comprehensive weekly brief:")
            brief = digest['brief_content']
            # Clean up markdown
            brief = brief.replace('#', '').replace('*', '').replace('_', '')
            lines.append(brief)
            lines.append("")

        # Key discussions
        if digest.get('key_discussions'):
            discussions = digest['key_discussions']
            if isinstance(discussions, list) and discussions:
                lines.append(f"This week featured {len(discussions)} key discussions in the community.")
                for i, discussion in enumerate(discussions, 1):
                    if isinstance(discussion, dict):
                        title = discussion.get('title', f'Discussion {i}')
                        content = discussion.get('summary', discussion.get('content', ''))
                        lines.append(f"Discussion {i}: {title}")
                        lines.append(content[:1000])  # Include more content
                    elif isinstance(discussion, str):
                        lines.append(discussion)
                    lines.append("")

        # Ledger activity
        if digest.get('ledger_activity'):
            lines.append("On-chain activity this week:")
            ledger = digest['ledger_activity']
            if isinstance(ledger, dict):
                for key, value in ledger.items():
                    lines.append(f"{key}: {value}")
            else:
                lines.append(str(ledger))
            lines.append("")

        # Community pulse
        if digest.get('community_pulse'):
            lines.append("Community pulse:")
            lines.append(str(digest['community_pulse']))
            lines.append("")

        # Themes
        if digest.get('themes'):
            themes = digest['themes']
            if isinstance(themes, list) and themes:
                lines.append(f"This week's key themes included: {', '.join(themes[:5])}")
                lines.append("")

        # Statistics
        if digest.get('statistics'):
            stats = digest['statistics']
            if isinstance(stats, dict):
                lines.append("Weekly statistics:")
                for key, value in list(stats.items())[:5]:
                    lines.append(f"{key}: {value}")
                lines.append("")

        # Closing
        lines.append("That concludes this week's comprehensive Regen Network digest.")
        lines.append("This digest covered important developments in regenerative finance, ecological economics, and community governance.")
        lines.append("To learn more and get involved, visit regen.network or join the discussion at forum.regen.network.")
        lines.append("Thank you for listening to the Pathway to Planetary Regeneration.")
        lines.append("Together, we're building a regenerative economy that works for people and planet.")

        return "\n".join(lines)

    def _generate_audio(self, script: str, digest: Dict) -> str:
        """Generate audio using OpenAI TTS"""

        # Output filename
        week_end = digest.get('week_end', datetime.now().strftime('%Y-%m-%d'))
        output_file = self.output_dir / f"regen_weekly_{week_end}.mp3"

        try:
            # Check if script is too long for a single request
            MAX_CHARS = 4000  # Leave some buffer below API limit of 4096

            if len(script) <= MAX_CHARS:
                # Single request for short scripts
                response = self.client.audio.speech.create(
                    model="tts-1",  # or "tts-1-hd" for higher quality
                    voice="nova",   # or "alloy", "echo", "fable", "onyx", "shimmer"
                    input=script,
                    speed=1.0       # 0.25 to 4.0
                )
                # Save to file
                response.stream_to_file(str(output_file))

            else:
                # Split script into chunks and generate multiple audio files
                logger.info(f"Script too long ({len(script)} chars), splitting into chunks...")

                # Smart split on paragraph boundaries
                paragraphs = script.split('\n\n')
                chunks = []
                current_chunk = ""

                for para in paragraphs:
                    # If adding this paragraph would exceed limit, save current chunk
                    if current_chunk and len(current_chunk) + len(para) + 2 > MAX_CHARS:
                        chunks.append(current_chunk)
                        current_chunk = para
                    else:
                        if current_chunk:
                            current_chunk += "\n\n" + para
                        else:
                            current_chunk = para

                # Add the last chunk
                if current_chunk:
                    chunks.append(current_chunk)

                logger.info(f"Split into {len(chunks)} chunks")

                # Generate audio for each chunk
                from pydub import AudioSegment
                combined = AudioSegment.empty()

                for i, chunk in enumerate(chunks, 1):
                    logger.info(f"Generating chunk {i}/{len(chunks)} ({len(chunk)} chars)...")
                    response = self.client.audio.speech.create(
                        model="tts-1",
                        voice="nova",
                        input=chunk,
                        speed=1.0
                    )

                    # Save chunk to temporary file
                    temp_file = self.output_dir / f"temp_chunk_{i}.mp3"
                    response.stream_to_file(str(temp_file))

                    # Load and append to combined audio
                    chunk_audio = AudioSegment.from_mp3(str(temp_file))
                    combined += chunk_audio

                    # Clean up temp file
                    temp_file.unlink()

                # Export combined audio
                combined.export(str(output_file), format="mp3")
                logger.info(f"Combined {len(chunks)} chunks into {output_file}")

            return str(output_file)

        except ImportError as ie:
            if "pydub" in str(ie):
                # If pydub not installed, fall back to truncating
                logger.warning("pydub not installed, truncating script to fit API limit")
                truncated_script = script[:MAX_CHARS-100] + "\n\n[Content truncated for audio generation]"

                response = self.client.audio.speech.create(
                    model="tts-1",
                    voice="nova",
                    input=truncated_script,
                    speed=1.0
                )
                response.stream_to_file(str(output_file))
                return str(output_file)
            else:
                raise

        except Exception as e:
            logger.error(f"Failed to generate audio: {e}")

            # Create placeholder file
            placeholder = output_file.with_suffix('.txt')
            with open(placeholder, 'w') as f:
                f.write(f"PODCAST SCRIPT\n{'='*50}\n\n{script}\n\n")
                f.write(f"Error generating audio: {e}\n")

            return str(placeholder)

def main():
    """Generate podcast from weekly digest"""
    import argparse
    import sys

    parser = argparse.ArgumentParser(description='Generate podcast from weekly digest')
    parser.add_argument('digest_file', nargs='?', help='Path to digest JSON file')
    parser.add_argument('--output-dir', default='podcast_audio', help='Output directory for audio files')

    args = parser.parse_args()

    # If no file provided, look for latest digest
    if args.digest_file:
        digest_path = Path(args.digest_file)
        if not digest_path.exists():
            print(f"❌ Digest file not found: {digest_path}")
            sys.exit(1)
    else:
        # Find latest digest
        digest_dir = Path("output/weekly")
        json_files = list(digest_dir.glob("weekly_digest_*.json"))
        if not json_files:
            print("No weekly digest found!")
            sys.exit(1)
        digest_path = max(json_files, key=lambda x: x.stat().st_mtime)

    print(f"📚 Using digest: {digest_path}")

    # Generate podcast
    try:
        generator = SimplePodcastGenerator()

        # Override output directory if specified
        if args.output_dir:
            generator.output_dir = Path(args.output_dir)
            generator.output_dir.mkdir(exist_ok=True)

        audio_file = generator.generate_from_digest(str(digest_path))
        print(f"✅ Podcast generated: {audio_file}")

        # Show file info
        if audio_file.endswith('.mp3'):
            size_mb = Path(audio_file).stat().st_size / (1024 * 1024)
            print(f"📊 File size: {size_mb:.1f} MB")

        # Return success
        sys.exit(0)
    except Exception as e:
        print(f"❌ Failed to generate podcast: {e}")
        print("\n💡 Make sure OPENAI_API_KEY is set in .env file")
        sys.exit(1)

if __name__ == "__main__":
    main()