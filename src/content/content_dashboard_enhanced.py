#!/usr/bin/env python3
"""
Enhanced Milestone B Content Operations Dashboard
Web-based interface with manual digest generation capabilities
"""

from flask import Flask, render_template, jsonify, request, session, redirect, url_for
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import sys
import json
import yaml
import subprocess
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional
import asyncio
from functools import wraps
from loguru import logger

# Add parent directories to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "koi-processor"))

# Initialize Flask app
app = Flask(__name__,
            template_folder='../../templates',
            static_folder='../../static')
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'dev-secret-key-milestone-b')
CORS(app, resources={r"/*": {"origins": "*"}})
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Configuration
CONFIG_PATH = Path(__file__).parent.parent / "config" / "dashboard_config.yaml"
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', '5433')),
    'database': os.environ.get('DB_NAME', 'eliza'),
    'user': os.environ.get('DB_USER', 'postgres'),
    'password': os.environ.get('DB_PASSWORD', 'postgres')
}

# Load configuration
def load_config():
    """Load dashboard configuration"""
    config_file = Path(__file__).parent.parent / "config" / "dashboard_config.yaml"
    if config_file.exists():
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    else:
        return {
            'dashboard': {
                'port': 8400,
                'refresh_interval': 30,
                'auth_enabled': False
            }
        }

config = load_config()

# Database connection
def get_db_connection():
    """Create a database connection"""
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

# Authentication decorator
def require_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if config['dashboard'].get('auth_enabled', False):
            if 'user' not in session:
                return jsonify({'error': 'Authentication required'}), 401
        return f(*args, **kwargs)
    return decorated_function

# Routes
@app.route('/')
@require_auth
def index():
    """Main dashboard page"""
    return render_template('dashboard_enhanced.html', config=config)

@app.route('/api/dashboard/overview')
@require_auth
def get_overview():
    """Get overall system health and metrics"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get KOI memories count from last 24 hours
        cur.execute("""
            SELECT COUNT(*) as count, source_sensor
            FROM koi_memories
            WHERE created_at > NOW() - INTERVAL '24 hours'
            GROUP BY source_sensor
        """)
        recent_content = cur.fetchall()

        # Get today's content count
        cur.execute("""
            SELECT COUNT(*) as today_count
            FROM koi_memories
            WHERE DATE(created_at) = CURRENT_DATE
        """)
        today_count = cur.fetchone()['today_count']

        # Get quality metrics if available
        cur.execute("""
            SELECT AVG(quality_score) as avg_quality,
                   COUNT(*) as total_reviews
            FROM content_reviews
            WHERE created_at > NOW() - INTERVAL '7 days'
        """)
        quality_stats = cur.fetchone()

        cur.close()
        conn.close()

        return jsonify({
            'success': True,
            'overview': {
                'today_content': today_count,
                'recent_sources': recent_content,
                'quality_stats': quality_stats,
                'services': {
                    'koi_coordinator': check_service_status(8005),
                    'event_bridge': check_service_status(8100),
                    'bge_server': check_service_status(8090),
                    'mcp_server': check_service_status(8200)
                }
            },
            'timestamp': datetime.now(timezone.utc).isoformat()
        })

    except Exception as e:
        logger.error(f"Error getting overview: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

def check_service_status(port):
    """Check if a service is running on a given port"""
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(1)
    result = sock.connect_ex(('localhost', port))
    sock.close()
    return result == 0

@app.route('/api/dashboard/generate/daily', methods=['POST'])
@require_auth
def generate_daily():
    """Manually trigger daily digest generation"""
    try:
        # Get request data
        data = request.json or {}
        draft_mode = data.get('draft_mode', True)

        # Start generation in background thread
        thread = threading.Thread(
            target=run_daily_generation,
            args=(draft_mode,)
        )
        thread.start()

        return jsonify({
            'success': True,
            'message': 'Daily digest generation started',
            'draft_mode': draft_mode
        })

    except Exception as e:
        logger.error(f"Error triggering daily generation: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/dashboard/generate/weekly', methods=['POST'])
@require_auth
def generate_weekly():
    """Manually trigger weekly digest generation"""
    try:
        # Get request data
        data = request.json or {}
        draft_mode = data.get('draft_mode', True)

        # Start generation in background thread
        thread = threading.Thread(
            target=run_weekly_generation,
            args=(draft_mode,)
        )
        thread.start()

        return jsonify({
            'success': True,
            'message': 'Weekly digest generation started',
            'draft_mode': draft_mode
        })

    except Exception as e:
        logger.error(f"Error triggering weekly generation: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

def run_daily_generation(draft_mode=True):
    """Run daily curator generation"""
    try:
        # Emit start event
        socketio.emit('generation_started', {
            'type': 'daily',
            'timestamp': datetime.now().isoformat()
        })

        # Prepare output path
        output_dir = Path("/opt/projects/koi-processor/output/daily_threads")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"thread_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

        # Run the daily curator script
        cmd = [
            sys.executable,
            "/opt/projects/koi-processor/scripts/run_daily_curator.py",
            "daily",
            "--output", str(output_file),
            "--verbose"
        ]

        # Execute command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd="/opt/projects/koi-processor"
        )

        if result.returncode == 0:
            # Load the generated content
            if output_file.exists():
                with open(output_file, 'r') as f:
                    content = json.load(f)

                # Submit for review if not in draft mode
                if not draft_mode:
                    submit_for_review(str(output_file), 'daily')

                # Emit success event with content
                socketio.emit('generation_completed', {
                    'type': 'daily',
                    'success': True,
                    'content': content,
                    'output_file': str(output_file),
                    'timestamp': datetime.now().isoformat()
                })
            else:
                raise Exception("Output file not created")
        else:
            raise Exception(f"Generation failed: {result.stderr}")

    except Exception as e:
        logger.error(f"Daily generation error: {e}")
        socketio.emit('generation_error', {
            'type': 'daily',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        })

def run_weekly_generation(draft_mode=True):
    """Run weekly aggregator generation"""
    try:
        # Emit start event
        socketio.emit('generation_started', {
            'type': 'weekly',
            'timestamp': datetime.now().isoformat()
        })

        # Prepare output path
        output_dir = Path("/opt/projects/koi-processor/output/weekly_digests")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Run the weekly aggregator script
        cmd = [
            sys.executable,
            "/opt/projects/koi-processor/scripts/run_weekly_aggregator.py",
            "--output", str(output_dir)
        ]

        # Execute command
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd="/opt/projects/koi-processor"
        )

        if result.returncode == 0:
            # Find the generated files
            json_file = output_dir / f"weekly_digest_{datetime.now().strftime('%Y-%m-%d')}.json"
            md_file = output_dir / f"weekly_digest_{datetime.now().strftime('%Y-%m-%d')}.md"

            if json_file.exists():
                with open(json_file, 'r') as f:
                    content = json.load(f)

                # Submit for review if not in draft mode
                if not draft_mode:
                    submit_for_review(str(json_file), 'weekly')

                # Emit success event with content
                socketio.emit('generation_completed', {
                    'type': 'weekly',
                    'success': True,
                    'content': content,
                    'output_files': {
                        'json': str(json_file),
                        'markdown': str(md_file) if md_file.exists() else None
                    },
                    'timestamp': datetime.now().isoformat()
                })
            else:
                raise Exception("Output files not created")
        else:
            raise Exception(f"Generation failed: {result.stderr}")

    except Exception as e:
        logger.error(f"Weekly generation error: {e}")
        socketio.emit('generation_error', {
            'type': 'weekly',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        })

def submit_for_review(file_path, content_type):
    """Submit generated content for review"""
    try:
        cmd = [
            sys.executable,
            "/opt/projects/koi-processor/scripts/submit_for_review.py",
            file_path,
            "--type", content_type,
            "--auto-review"
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd="/opt/projects/koi-processor"
        )

        if result.returncode == 0:
            logger.info(f"Content submitted for review: {file_path}")
        else:
            logger.error(f"Review submission failed: {result.stderr}")

    except Exception as e:
        logger.error(f"Error submitting for review: {e}")

@app.route('/api/dashboard/drafts/<content_type>')
@require_auth
def get_drafts(content_type):
    """Get recent drafts for a content type"""
    try:
        output_dir = Path("/opt/projects/koi-processor/output")

        if content_type == 'daily':
            draft_dir = output_dir / "daily_threads"
        elif content_type == 'weekly':
            draft_dir = output_dir / "weekly_digests"
        else:
            return jsonify({'error': 'Invalid content type'}), 400

        drafts = []
        if draft_dir.exists():
            # Get recent JSON files
            json_files = sorted(draft_dir.glob('*.json'), key=lambda x: x.stat().st_mtime, reverse=True)[:10]

            for file_path in json_files:
                try:
                    with open(file_path, 'r') as f:
                        content = json.load(f)

                    drafts.append({
                        'filename': file_path.name,
                        'path': str(file_path),
                        'created': datetime.fromtimestamp(file_path.stat().st_mtime).isoformat(),
                        'content': content
                    })
                except Exception as e:
                    logger.error(f"Error reading draft {file_path}: {e}")

        return jsonify({
            'success': True,
            'drafts': drafts,
            'count': len(drafts)
        })

    except Exception as e:
        logger.error(f"Error getting drafts: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/dashboard/status')
def get_status():
    """Get current system status"""
    return jsonify({
        'success': True,
        'status': {
            'dashboard': 'running',
            'websocket': socketio.server.eio.connected,
            'database': test_db_connection(),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    })

def test_db_connection():
    """Test database connectivity"""
    try:
        conn = get_db_connection()
        conn.close()
        return True
    except:
        return False

# WebSocket events
@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    logger.info(f"Client connected: {request.sid}")
    emit('connected', {'data': 'Connected to dashboard'})

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    logger.info(f"Client disconnected: {request.sid}")

@socketio.on('request_update')
def handle_update_request(data):
    """Handle update request from client"""
    emit('update_response', {
        'timestamp': datetime.now().isoformat(),
        'data': 'Update triggered'
    })

if __name__ == '__main__':
    logger.info(f"Starting Enhanced Content Dashboard on port {config['dashboard']['port']}")
    socketio.run(app,
                 host='0.0.0.0',
                 port=config['dashboard']['port'],
                 debug=False,
                 allow_unsafe_werkzeug=True)