#!/usr/bin/env python3
"""
KOI Agent Knowledge Permissions API
Provides REST API endpoints for managing agent knowledge access permissions
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
import os
import uuid
from datetime import datetime
import json

app = Flask(__name__)
CORS(app)

# Database configuration
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', '5433')),
    'database': os.environ.get('DB_NAME', 'eliza'),
    'user': os.environ.get('DB_USER', 'postgres'),
    'password': os.environ.get('DB_PASSWORD', 'postgres')
}

def get_db_connection():
    """Create a database connection"""
    return psycopg2.connect(**DB_CONFIG, cursor_factory=RealDictCursor)

@app.route('/api/koi/permissions/agents', methods=['GET'])
def get_agents():
    """Get list of all agents with their current permissions"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get all agents with permission counts
        cur.execute("""
            SELECT 
                a.id,
                a.name,
                COUNT(DISTINCT akp.id) as permission_count,
                COUNT(DISTINCT CASE WHEN akp.permission = 'allow' THEN akp.id END) as allow_count,
                COUNT(DISTINCT CASE WHEN akp.permission = 'deny' THEN akp.id END) as deny_count
            FROM agents a
            LEFT JOIN agent_knowledge_permissions akp ON a.id = akp.agent_id
            GROUP BY a.id, a.name
            ORDER BY a.name
        """)
        
        agents = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'agents': agents
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/koi/permissions/agent/<agent_id>', methods=['GET'])
def get_agent_permissions(agent_id):
    """Get permissions for a specific agent"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Validate agent exists
        cur.execute("SELECT id, name FROM agents WHERE id = %s", (agent_id,))
        agent = cur.fetchone()
        
        if not agent:
            return jsonify({
                'success': False,
                'error': 'Agent not found'
            }), 404
        
        # Get permissions
        cur.execute("""
            SELECT 
                id,
                source_type,
                source_identifier,
                permission,
                metadata,
                created_at,
                updated_at
            FROM agent_knowledge_permissions
            WHERE agent_id = %s
            ORDER BY source_type, source_identifier
        """, (agent_id,))
        
        permissions = cur.fetchall()
        
        # Get available data sources (RIDs from memories)
        cur.execute("""
            SELECT DISTINCT 
                content->>'rid' as rid,
                content->>'source_type' as source_type,
                content->>'source_file' as source_file,
                COUNT(*) as count
            FROM memories
            WHERE content->>'rid' IS NOT NULL
            GROUP BY content->>'rid', content->>'source_type', content->>'source_file'
            ORDER BY content->>'rid'
        """)
        
        available_sources = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'agent': agent,
            'permissions': permissions,
            'available_sources': available_sources
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/koi/permissions/agent/<agent_id>', methods=['POST'])
def update_agent_permissions(agent_id):
    """Update permissions for an agent"""
    try:
        data = request.json
        permissions = data.get('permissions', [])
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Validate agent exists
        cur.execute("SELECT id FROM agents WHERE id = %s", (agent_id,))
        if not cur.fetchone():
            return jsonify({
                'success': False,
                'error': 'Agent not found'
            }), 404
        
        # Clear existing permissions for this agent
        cur.execute("DELETE FROM agent_knowledge_permissions WHERE agent_id = %s", (agent_id,))
        
        # Insert new permissions
        for perm in permissions:
            cur.execute("""
                INSERT INTO agent_knowledge_permissions 
                (agent_id, source_type, source_identifier, permission, metadata)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                agent_id,
                perm.get('source_type'),
                perm.get('source_identifier'),
                perm.get('permission', 'allow'),
                json.dumps(perm.get('metadata', {}))
            ))
        
        conn.commit()
        
        # Get updated permissions
        cur.execute("""
            SELECT * FROM agent_knowledge_permissions
            WHERE agent_id = %s
            ORDER BY source_type, source_identifier
        """, (agent_id,))
        
        updated_permissions = cur.fetchall()
        
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Updated {len(permissions)} permissions for agent',
            'permissions': updated_permissions
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/koi/permissions/sources', methods=['GET'])
def get_data_sources():
    """Get all available data sources"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get unique RIDs and their metadata
        cur.execute("""
            SELECT 
                content->>'rid' as rid,
                content->>'source_type' as source_type,
                MIN(content->>'source_file') as source_file,
                MIN(content->>'url') as url,
                COUNT(*) as memory_count,
                MIN("createdAt") as first_seen,
                MAX("createdAt") as last_seen
            FROM memories
            WHERE content->>'rid' IS NOT NULL
            GROUP BY 
                content->>'rid',
                content->>'source_type'
            ORDER BY content->>'rid'
        """)
        
        sources = cur.fetchall()
        
        # Group by source type
        grouped = {}
        for source in sources:
            source_type = source.get('source_type', 'unknown')
            if source_type not in grouped:
                grouped[source_type] = []
            grouped[source_type].append(source)
        
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'sources': sources,
            'grouped': grouped,
            'total_count': len(sources)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/koi/permissions/test/<agent_id>', methods=['POST'])
def test_agent_access(agent_id):
    """Test if an agent has access to a specific RID"""
    try:
        data = request.json
        rid = data.get('rid')
        
        if not rid:
            return jsonify({
                'success': False,
                'error': 'RID is required'
            }), 400
        
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Use the agent_has_access function
        cur.execute("SELECT agent_has_access(%s::uuid, %s) as has_access", (agent_id, rid))
        result = cur.fetchone()
        
        # Get the allowed patterns for context
        cur.execute("SELECT get_agent_allowed_patterns(%s::uuid) as patterns", (agent_id,))
        patterns_result = cur.fetchone()
        
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'agent_id': agent_id,
            'rid': rid,
            'has_access': result['has_access'],
            'allowed_patterns': patterns_result['patterns']
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/koi/permissions/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Check if permissions table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'agent_knowledge_permissions'
            )
        """)
        
        table_exists = cur.fetchone()['exists']
        
        if table_exists:
            cur.execute("SELECT COUNT(*) as count FROM agent_knowledge_permissions")
            permission_count = cur.fetchone()['count']
        else:
            permission_count = 0
        
        cur.close()
        conn.close()
        
        return jsonify({
            'success': True,
            'status': 'healthy',
            'table_exists': table_exists,
            'permission_count': permission_count
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'status': 'unhealthy',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', '8300'))
    print(f"Starting KOI Permissions API on port {port}")
    app.run(host='0.0.0.0', port=port, debug=True)