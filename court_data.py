import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_db_connection():
    """Create a database connection"""
    return psycopg2.connect(os.environ['DATABASE_URL'])

def initialize_database():
    """Create the courts table and scraper status table"""
    conn = get_db_connection()
    cur = conn.cursor()

    # Create courts table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS courts (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            type VARCHAR(50) NOT NULL,
            status VARCHAR(50) NOT NULL,
            lat FLOAT NOT NULL,
            lon FLOAT NOT NULL,
            address TEXT NOT NULL,
            image_url TEXT NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS scraper_status (
            id SERIAL PRIMARY KEY,
            start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            end_time TIMESTAMP,
            courts_processed INTEGER DEFAULT 0,
            total_courts INTEGER,
            status VARCHAR(50) DEFAULT 'running',
            message TEXT,
            current_court TEXT,
            next_court TEXT,
            stage TEXT
        );

        CREATE TABLE IF NOT EXISTS scraper_logs (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            level VARCHAR(20) NOT NULL,
            message TEXT NOT NULL,
            scraper_run_id INTEGER REFERENCES scraper_status(id)
        );

        CREATE TABLE IF NOT EXISTS api_usage (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            endpoint VARCHAR(50) NOT NULL,
            tokens_used INTEGER NOT NULL,
            model VARCHAR(50) NOT NULL,
            success BOOLEAN NOT NULL,
            error_message TEXT
        );
    """)

    conn.commit()
    cur.close()
    conn.close()

def get_court_data():
    """Get all court data from the database"""
    # Define expected columns
    expected_columns = [
        'id', 'name', 'type', 'status', 'lat', 'lon', 
        'address', 'image_url', 'last_updated'
    ]

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT * FROM courts ORDER BY name")
    data = cur.fetchall()

    cur.close()
    conn.close()

    # Return DataFrame with consistent columns
    if data:
        df = pd.DataFrame(data)
        # Ensure all expected columns exist
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None
        return df[expected_columns]  # Return only expected columns in specific order
    else:
        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=expected_columns)

def get_scraper_status():
    """Get the latest scraper status"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT * FROM scraper_status 
        ORDER BY start_time DESC 
        LIMIT 1
    """)
    status = cur.fetchone()

    cur.close()
    conn.close()

    return status or {
        'status': 'not_started',
        'courts_processed': 0,
        'total_courts': 0,
        'message': 'Scraper has not been started',
        'start_time': None,
        'end_time': None,
        'current_court': None,
        'next_court': None,
        'stage': None
    }

def get_scraper_logs(limit=50):
    """Get the most recent scraper logs"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT timestamp, level, message 
        FROM scraper_logs 
        ORDER BY timestamp DESC 
        LIMIT %s
    """, (limit,))
    logs = cur.fetchall()

    cur.close()
    conn.close()

    return logs

def add_scraper_log(level, message, scraper_run_id=None):
    """Add a new scraper log entry"""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO scraper_logs (level, message, scraper_run_id)
        VALUES (%s, %s, %s)
    """, (level, message, scraper_run_id))

    conn.commit()
    cur.close()
    conn.close()

def update_scraper_status(scraper_run_id: int, sources_processed: int, total_sources: int, 
                         status: str, message: str, current_court: str = None, 
                         next_court: str = None, stage: str = None):
    """Updates the status of the scraper run with proper parameter handling."""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        if status == 'completed':
            cur.execute("""
                UPDATE inventory_updates 
                SET sources_processed = %s,
                    total_sources = %s,
                    status = %s,
                    message = %s,
                    current_court = %s,
                    next_court = %s,
                    stage = %s,
                    end_time = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (sources_processed, total_sources, status, message, 
                  current_court, next_court, stage, scraper_run_id))
        else:
            cur.execute("""
                UPDATE inventory_updates
                SET sources_processed = %s,
                    total_sources = %s,
                    status = %s,
                    message = %s,
                    current_court = %s,
                    next_court = %s,
                    stage = %s
                WHERE id = %s
            """, (sources_processed, total_sources, status, message,
                  current_court, next_court, stage, scraper_run_id))

        conn.commit()
    except Exception as e:
        logger.error(f"Error updating scraper status: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def get_court_types():
    """Get unique court types from the database"""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT type FROM courts ORDER BY type")
    types = [row[0] for row in cur.fetchall()]

    cur.close()
    conn.close()

    return types or ['Supreme Court', 'Appeals Court', 'District Court', 'Bankruptcy Court', 'Other']

def get_court_statuses():
    """Get unique court statuses from the database"""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT status FROM courts ORDER BY status")
    statuses = [row[0] for row in cur.fetchall()]

    cur.close()
    conn.close()

    return statuses or ['Open', 'Closed', 'Limited Operations']

def log_api_usage(endpoint: str, tokens_used: int, model: str, success: bool, error_message: str = None):
    """Log OpenAI API usage"""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO api_usage (endpoint, tokens_used, model, success, error_message)
        VALUES (%s, %s, %s, %s, %s)
    """, (endpoint, tokens_used, model, success, error_message))

    conn.commit()
    cur.close()
    conn.close()

def get_api_usage_stats():
    """Get API usage statistics"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Get overall statistics
    cur.execute("""
        SELECT 
            COUNT(*) as total_calls,
            SUM(tokens_used) as total_tokens,
            COUNT(*) FILTER (WHERE success = true) as successful_calls,
            COUNT(*) FILTER (WHERE success = false) as failed_calls,
            MAX(timestamp) as last_call_time
        FROM api_usage
    """)
    overall_stats = cur.fetchone()

    # Get usage by model
    cur.execute("""
        SELECT 
            model,
            COUNT(*) as calls,
            SUM(tokens_used) as tokens
        FROM api_usage
        GROUP BY model
        ORDER BY calls DESC
    """)
    model_stats = cur.fetchall()

    # Get recent calls
    cur.execute("""
        SELECT *
        FROM api_usage
        ORDER BY timestamp DESC
        LIMIT 50
    """)
    recent_calls = cur.fetchall()

    cur.close()
    conn.close()

    return {
        'overall': overall_stats,
        'by_model': model_stats,
        'recent': recent_calls
    }

def get_filtered_court_data(filters=None):
    """Get court data with optional filters"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    query = """
        SELECT 
            c.id, c.name, c.type, c.status, c.address, c.lat, c.lon,
            j.name as jurisdiction_name, j.type as jurisdiction_type,
            p.name as parent_jurisdiction
        FROM courts c
        LEFT JOIN jurisdictions j ON c.jurisdiction_id = j.id
        LEFT JOIN jurisdictions p ON j.parent_id = p.id
        WHERE 1=1
    """
    params = []

    if filters:
        if filters.get('status'):
            query += " AND c.status = %s"
            params.append(filters['status'])

        if filters.get('type'):
            query += " AND c.type = %s"
            params.append(filters['type'])

        if filters.get('jurisdiction'):
            query += " AND (j.name = %s OR p.name = %s)"
            params.extend([filters['jurisdiction'], filters['jurisdiction']])

        if filters.get('search'):
            query += " AND (c.name ILIKE %s OR c.address ILIKE %s)"
            search_term = f"%{filters['search']}%"
            params.extend([search_term, search_term])

    query += " ORDER BY c.name"

    cur.execute(query, params)
    data = cur.fetchall()

    cur.close()
    conn.close()

    if data:
        df = pd.DataFrame(data)
        return df
    else:
        return pd.DataFrame(columns=[
            'id', 'name', 'type', 'status', 'address', 'lat', 'lon',
            'jurisdiction_name', 'jurisdiction_type', 'parent_jurisdiction'
        ])


# Initialize the database when the module is imported
initialize_database()