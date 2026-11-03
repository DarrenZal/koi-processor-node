#!/usr/bin/env python3
"""
Podcastfy Generator for Regen Network Weekly Digest

Automated podcast audio generation using Podcastfy library.
Creates conversational audio from weekly digests without requiring NotebookLM.
"""

import os
import sys
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
import tempfile
import shutil
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Podcastfy imports
try:
    import podcastfy
    from podcastfy.client import PodcastClient
    PODCASTFY_AVAILABLE = True
except ImportError:
    try:
        # Try alternative import
        import podcastfy
        PODCASTFY_AVAILABLE = True
        # Create a simple wrapper if the client isn't available
        class PodcastClient:
            def __init__(self, *args, **kwargs):
                pass
            def create_podcast(self, *args, **kwargs):
                raise NotImplementedError("Podcastfy client not properly installed")
    except ImportError:
        PODCASTFY_AVAILABLE = False
        print("Warning: Podcastfy not installed. Install with: pip install podcastfy")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PodcastfyGenerator:
    """
    Generates podcast audio using Podcastfy library
    """
    
    def __init__(self, config_path: str = "config/podcastfy_config.json"):
        """Initialize the Podcastfy generator"""
        self.config = self._load_config(config_path)
        
        if not PODCASTFY_AVAILABLE:
            raise ImportError("Podcastfy is required. Install with: pip install podcastfy")
        
        # Initialize Podcastfy with configuration
        self.podcastfy = self._initialize_podcastfy()
        
        # Output paths
        self.output_path = Path(self.config.get("output_path", "./podcast_audio"))
        self.output_path.mkdir(parents=True, exist_ok=True)
        
        # Audio settings
        self.audio_config = self.config.get("audio", {
            "format": "mp3",
            "quality": "high",
            "target_duration": 20,  # minutes
            "voices": {
                "host": "en-US-Neural2-J",  # Male voice
                "cohost": "en-US-Neural2-C"  # Female voice
            }
        })
    
    def _load_config(self, config_path: str) -> Dict:
        """Load or create default configuration"""
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                return json.load(f)
        else:
            # Default configuration
            default_config = {
                "output_path": "./podcast_audio",
                "podcastfy": {
                    "model": "gpt-4o-mini",  # or "claude-3-haiku" for faster/cheaper
                    "api_key_env": "OPENAI_API_KEY",  # or "ANTHROPIC_API_KEY"
                    "tts_provider": "openai",  # or "elevenlabs", "google"
                    "tts_model": "tts-1",
                    "conversation_style": "informative_engaging",
                    "language": "en"
                },
                "audio": {
                    "format": "mp3",
                    "quality": "high",
                    "target_duration": 20,
                    "voices": {
                        "host": "alloy",
                        "cohost": "nova"
                    }
                },
                "content": {
                    "intro_style": "brief",
                    "outro_style": "call_to_action",
                    "segment_transitions": True,
                    "include_soundscape": False
                }
            }
            
            # Save default config
            os.makedirs(os.path.dirname(config_path) or ".", exist_ok=True)
            with open(config_path, 'w') as f:
                json.dump(default_config, f, indent=2)
            
            return default_config
    
    def _initialize_podcastfy(self):
        """Initialize Podcastfy with configuration"""
        podcastfy_config = self.config.get("podcastfy", {})
        
        # Get API key from environment
        api_key_env = podcastfy_config.get("api_key_env", "OPENAI_API_KEY")
        api_key = os.environ.get(api_key_env)
        
        if not api_key:
            raise ValueError(f"API key not found in environment variable: {api_key_env}")
        
        # Initialize Podcastfy client
        try:
            # Try using the client if available
            client = PodcastClient(
                api_key=api_key,
                model=podcastfy_config.get("model", "gpt-4o-mini"),
                tts_provider=podcastfy_config.get("tts_provider", "openai"),
                tts_model=podcastfy_config.get("tts_model", "tts-1")
            )
        except:
            # Fallback to basic initialization
            client = None
            logger.warning("Podcastfy client initialization failed, using fallback")

        return client
    
    def generate_podcast_from_digest(self, 
                                    digest: Dict[str, Any],
                                    output_filename: Optional[str] = None) -> str:
        """
        Generate podcast audio from weekly digest
        
        Args:
            digest: Weekly digest data from aggregator
            output_filename: Optional output filename
        
        Returns:
            Path to generated audio file
        """
        logger.info("Starting Podcastfy audio generation")
        
        # Prepare content for Podcastfy
        content = self._prepare_content(digest)
        
        # Generate conversation script
        conversation = self._generate_conversation(content)
        
        # Set output filename
        if not output_filename:
            week_num = datetime.now().isocalendar()[1]
            output_filename = f"regen_weekly_ep{week_num:02d}_{datetime.now().strftime('%Y%m%d')}.mp3"
        
        output_file = self.output_path / output_filename
        
        # Generate audio using Podcastfy
        try:
            logger.info("Generating audio with Podcastfy...")
            
            # Create temporary file for content
            with tempfile.NamedTemporaryFile(mode='w', suffix='.md', delete=False) as tmp:
                tmp.write(conversation)
                tmp_path = tmp.name
            
            # Generate podcast
            audio_path = self.podcastfy.create_podcast(
                input_file=tmp_path,
                output_file=str(output_file),
                conversation_style=self.config["podcastfy"].get("conversation_style", "informative_engaging"),
                voices=self.audio_config["voices"]
            )
            
            # Clean up temp file
            os.unlink(tmp_path)
            
            logger.info(f"✅ Audio generated successfully: {audio_path}")
            return audio_path
            
        except Exception as e:
            logger.error(f"Podcastfy generation failed: {e}")
            raise
    
    def _prepare_content(self, digest: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare digest content for podcast generation"""
        
        # Extract key information
        content = {
            "title": f"Regen Network Weekly - {digest.get('theme', 'Weekly Update')}",
            "week_ending": digest.get("week_ending", datetime.now().isoformat()),
            "executive_summary": digest.get("executive_summary", ""),
            "main_topics": [],
            "statistics": digest.get("stats", {}),
            "call_to_action": self._generate_cta()
        }
        
        # Process top stories
        for story in digest.get("top_stories", [])[:5]:
            topic = {
                "headline": story.get("title", ""),
                "summary": story.get("summary", ""),
                "significance": story.get("significance", ""),
                "source": story.get("source", "")
            }
            content["main_topics"].append(topic)
        
        # Add themes
        content["themes"] = digest.get("themes", [])[:5]
        
        # Add key statistics
        if "stats" in digest:
            stats = digest["stats"]
            content["key_numbers"] = {
                "total_content": stats.get("total_content", 0),
                "top_sources": stats.get("top_sources", []),
                "active_proposals": stats.get("governance_proposals", 0),
                "new_credits": stats.get("new_credits_issued", 0)
            }
        
        return content
    
    def _generate_conversation(self, content: Dict[str, Any]) -> str:
        """Generate conversational script from content"""
        
        script = []
        
        # Introduction
        script.append(f"# {content['title']}")
        script.append("")
        script.append("## Introduction")
        script.append(content["executive_summary"])
        script.append("")
        
        # Main topics
        script.append("## This Week's Top Stories")
        script.append("")
        
        for i, topic in enumerate(content["main_topics"], 1):
            script.append(f"### Story {i}: {topic['headline']}")
            script.append(topic["summary"])
            if topic.get("significance"):
                script.append(f"**Why this matters:** {topic['significance']}")
            script.append("")
        
        # Themes
        if content.get("themes"):
            script.append("## Key Themes This Week")
            for theme in content["themes"]:
                script.append(f"- {theme}")
            script.append("")
        
        # Statistics
        if content.get("key_numbers"):
            script.append("## By the Numbers")
            numbers = content["key_numbers"]
            script.append(f"- {numbers['total_content']} pieces of content analyzed")
            if numbers.get("active_proposals"):
                script.append(f"- {numbers['active_proposals']} active governance proposals")
            if numbers.get("new_credits"):
                script.append(f"- {numbers['new_credits']} new carbon credits issued")
            script.append("")
        
        # Call to action
        script.append("## Get Involved")
        script.append(content["call_to_action"])
        
        # Add conversation hints for Podcastfy
        script.append("")
        script.append("---")
        script.append("CONVERSATION STYLE NOTES:")
        script.append("- Create a natural dialogue between two hosts")
        script.append("- Host 1 is more technical, Host 2 asks clarifying questions")
        script.append("- Include enthusiasm about positive developments")
        script.append("- Explain technical terms in accessible language")
        script.append("- Target duration: 20 minutes")
        
        return "\n".join(script)
    
    def _generate_cta(self) -> str:
        """Generate call to action for podcast ending"""
        return """
        To learn more and get involved with Regen Network:
        - Join the discussion at forum.regen.network
        - Explore carbon credits at registry.regen.network
        - Read the documentation at docs.regen.network
        - Follow us on Twitter @regen_network
        - Join our Discord community for real-time discussions
        
        Thanks for listening to the Regen Network Weekly Digest!
        """
    
    def generate_test_podcast(self) -> str:
        """Generate a test podcast with sample data"""
        
        # Create test digest
        test_digest = {
            "theme": "Test Episode - Regenerative Innovations",
            "week_ending": datetime.now().isoformat(),
            "executive_summary": "This is a test episode demonstrating the Podcastfy integration for automated podcast generation.",
            "top_stories": [
                {
                    "title": "New Carbon Methodology Approved",
                    "summary": "A groundbreaking soil carbon methodology has been approved, enabling farmers to earn credits for regenerative practices.",
                    "significance": "This opens up new revenue streams for regenerative farmers"
                },
                {
                    "title": "Governance Proposal 50 Passes",
                    "summary": "The community voted to allocate funds for ecosystem development.",
                    "significance": "Strengthens the ecosystem's growth trajectory"
                }
            ],
            "themes": ["Carbon Markets", "Governance", "Technology"],
            "stats": {
                "total_content": 42,
                "governance_proposals": 3,
                "new_credits_issued": 10000
            }
        }
        
        # Generate podcast
        return self.generate_podcast_from_digest(test_digest, "test_episode.mp3")


class PodcastfySimulator:
    """
    Simulator for testing when Podcastfy is not installed
    Creates a mock audio file for testing the pipeline
    """
    
    def __init__(self):
        self.output_path = Path("./podcast_audio")
        self.output_path.mkdir(parents=True, exist_ok=True)
    
    def generate_mock_audio(self, digest: Dict[str, Any], output_filename: str) -> str:
        """Generate a mock audio file for testing"""
        output_file = self.output_path / output_filename
        
        # Create a simple text file as placeholder
        content = f"""
        Mock Podcast Audio
        Generated: {datetime.now()}
        Theme: {digest.get('theme', 'Test')}
        Duration: 20:00
        
        This is a placeholder audio file for testing.
        In production, this would be actual audio generated by Podcastfy.
        """
        
        # Write as .txt for testing (would be .mp3 in production)
        test_file = output_file.with_suffix('.txt')
        with open(test_file, 'w') as f:
            f.write(content)
        
        logger.info(f"Mock audio created: {test_file}")
        return str(test_file)


def test_podcastfy_generator():
    """Test the Podcastfy generator"""
    
    print("=" * 50)
    print("PODCASTFY GENERATOR TEST")
    print("=" * 50)
    
    if PODCASTFY_AVAILABLE:
        try:
            # Use real Podcastfy
            generator = PodcastfyGenerator()
            audio_file = generator.generate_test_podcast()
            print(f"✅ Test podcast generated: {audio_file}")
        except Exception as e:
            print(f"❌ Podcastfy test failed: {e}")
            print("Falling back to simulator...")
            
            # Use simulator
            simulator = PodcastfySimulator()
            test_digest = {"theme": "Test Episode"}
            audio_file = simulator.generate_mock_audio(test_digest, "test_episode.mp3")
            print(f"✅ Mock audio generated: {audio_file}")
    else:
        print("Podcastfy not installed, using simulator...")
        simulator = PodcastfySimulator()
        test_digest = {"theme": "Test Episode"}
        audio_file = simulator.generate_mock_audio(test_digest, "test_episode.mp3")
        print(f"✅ Mock audio generated: {audio_file}")
    
    return audio_file


if __name__ == "__main__":
    # Run test
    if "--test" in sys.argv:
        test_file = test_podcastfy_generator()
        print(f"\nTest complete. Output: {test_file}")
    else:
        print("Usage: python podcastfy_generator.py --test")
        print("\nThis module provides automated podcast generation using Podcastfy.")
        print("Features:")
        print("- Converts weekly digests to conversational audio")
        print("- Configurable voices and conversation styles")
        print("- No manual NotebookLM step required")
        print("- Fully automated audio generation")