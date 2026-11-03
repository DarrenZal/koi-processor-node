#!/usr/bin/env python3
"""
KOI Content API - Serves real sensed content data from PostgreSQL
"""

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
import json
from typing import List, Dict, Any, Optional
import uvicorn
import os
import sys

# Add parent directory to path to allow importing from src
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import auth components
from src.services.auth_service import router as auth_router, get_authorized_user

app = FastAPI(title="KOI Content API", version="1.0.0")

# Enable CORS for browser access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include Auth Router
app.include_router(auth_router, prefix="/api/koi")

# Database connection: read from the environment (.env via the systemd unit)
POSTGRES_URL = os.environ.get("POSTGRES_URL")
if not POSTGRES_URL:
    raise RuntimeError("POSTGRES_URL is not set; koi_content_api reads its database connection from the environment")

def get_db_connection():
    """Create database connection"""
    return psycopg2.connect(POSTGRES_URL, cursor_factory=RealDictCursor)

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "KOI Content API"}

@app.post("/api/koi/search")
async def search(
    query: str,
    user_email: str = Depends(get_authorized_user) 
):
    """
    Search KOI database.
    The get_authorized_user dependency enforces that the user has a valid OAuth token
    (proving they are @regen.network). If so, we include 'google_drive' results.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Simple keyword search across content
        # In production, this would use vector search (pgvector)
        
        # Construct source list - authenticating enables google_drive
        sources = ['discourse', 'docs', 'ledger', 'website', 'github', 'twitter', 'notion']
        if user_email:
            sources.append('google_drive')
            
        # SQL query
        sql_query = """
            SELECT 
                content, 
                metadata, 
                created_at 
            FROM koi_memories 
            WHERE 
                (content ILIKE %s OR metadata->>'title' ILIKE %s)
                AND (
                    metadata->>'source' = ANY(%s) 
                    OR (metadata->>'source' IS NULL AND 'unknown' = ANY(%s))
                )
            LIMIT 20
        """
        
        search_term = f"%{query}%"
        cur.execute(sql_query, (search_term, search_term, sources, sources))
        
        results = cur.fetchall()
        cur.close()
        conn.close()
        
        return {
            "query": query,
            "user": user_email,
            "count": len(results),
            "results": results
        }
        
    except Exception as e:
        # If get_authorized_user fails, it raises HTTPException(401) before reaching here.
        # Other errors are 500.
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/koi/content/domains")
async def get_domains(
    user_email: str = Depends(get_authorized_user)
):
    """Get all domains with content counts (authenticated)"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                metadata->>'domain' as domain,
                COUNT(*) as page_count
            FROM koi_memories
            WHERE metadata->>'domain' IS NOT NULL
            GROUP BY domain
            ORDER BY page_count DESC
        """)

        results = cur.fetchall()
        cur.close()
        conn.close()

        return {"domains": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/koi/content/pages/{domain}")
async def get_pages_by_domain(
    domain: str,
    user_email: str = Depends(get_authorized_user)
):
    """Get all pages sensed from a specific domain (website or discourse forum) (authenticated)"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Try both domain field and forum field for discourse compatibility
        cur.execute("""
            SELECT
                metadata->>'title' as title,
                metadata->>'url' as url,
                metadata->>'source_file' as source_file,
                created_at
            FROM koi_memories
            WHERE metadata->>'domain' = %s
               OR metadata->>'forum' = %s
            ORDER BY created_at DESC
        """, (domain, domain))

        results = cur.fetchall()
        cur.close()
        conn.close()

        return {"domain": domain, "pages": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/koi/content/notion")
async def get_notion_pages(
    user_email: str = Depends(get_authorized_user)
):
    """Get all Notion pages that have been sensed (authenticated)"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT DISTINCT
                metadata->>'title' as title,
                metadata->>'url' as url
            FROM koi_memories
            WHERE metadata->>'source' = 'notion'
            OR metadata->>'url' LIKE '%notion.so%'
            ORDER BY title
        """)

        results = cur.fetchall()
        cur.close()
        conn.close()

        return {"pages": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/koi/content/sources")
async def get_all_sources(
    user_email: str = Depends(get_authorized_user)
):
    """Get all sources grouped by sensor type (authenticated)"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Get website sources
        cur.execute("""
            SELECT DISTINCT
                'website' as sensor_type,
                metadata->>'domain' as source
            FROM koi_memories
            WHERE metadata->>'domain' IS NOT NULL
        """)
        website_sources = cur.fetchall()

        # Get GitHub sources
        cur.execute("""
            SELECT DISTINCT
                'github' as sensor_type,
                metadata->>'repository' as source
            FROM koi_memories
            WHERE metadata->>'repository' IS NOT NULL
        """)
        github_sources = cur.fetchall()

        # Get Twitter sources
        cur.execute("""
            SELECT DISTINCT
                'twitter' as sensor_type,
                metadata->>'author' as source
            FROM koi_memories
            WHERE metadata->>'source' = 'twitter'
            AND metadata->>'author' IS NOT NULL
        """)
        twitter_sources = cur.fetchall()

        cur.close()
        conn.close()

        # Group by sensor type
        sources_by_type = {}
        for item in website_sources + github_sources + twitter_sources:
            sensor_type = item['sensor_type']
            source = item['source']
            if sensor_type not in sources_by_type:
                sources_by_type[sensor_type] = []
            if source:
                sources_by_type[sensor_type].append(source)

        return {"sources": sources_by_type}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/koi/content/statistics")
async def get_statistics(
    user_email: str = Depends(get_authorized_user)
):
    """Get overall content statistics (authenticated)"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Total memories
        cur.execute("SELECT COUNT(*) as total FROM koi_memories")
        total = cur.fetchone()['total']

        # By source type
        cur.execute("""
            SELECT
                metadata->>'source' as source,
                COUNT(*) as count
            FROM koi_memories
            WHERE metadata->>'source' IS NOT NULL
            GROUP BY source
        """)
        by_source = cur.fetchall()

        # Recent activity (last 24 hours)
        cur.execute("""
            SELECT COUNT(*) as recent
            FROM koi_memories
            WHERE created_at > NOW() - INTERVAL '24 hours'
        """)
        recent = cur.fetchone()['recent']

        cur.close()
        conn.close()

        return {
            "total_memories": total,
            "by_source": by_source,
            "recent_24h": recent
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    # Run on port 8007. Bind to 0.0.0.0 so a reverse proxy running in a container can
    # reach us through the host bridge gateway; it proxies the /api/koi/auth/,
    # /api/koi/activate and /api/koi/content/ routes. Keep the port closed to the public
    # with a host firewall. Application-layer auth (Depends(get_authorized_user)) gates
    # the content and auth endpoints; firewall plus auth give defense in depth.
    uvicorn.run(app, host="0.0.0.0", port=8007)
