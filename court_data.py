import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import pool
import os
from datetime import datetime
import logging
import time
from typing import Optional, Dict, Any
from urllib.parse import urlparse

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Initialize connection pool
min_connections = 1
max_connections = 20
connection_pool = None

def init_connection_pool():
    """Initialize the database connection pool"""
    global connection_pool
    try:
        # Parse DATABASE_URL to extract host
        url = urlparse(os.environ['DATABASE_URL'])

        # Create connection pool with explicit IPv4 settings
        connection_pool = pool.SimpleConnectionPool(
            min_connections,
            max_connections,
            host=url.hostname,
            port=url.port or 5432,
            user=url.username,
            password=url.password,
            database=url.path[1:],  # Remove leading slash
            # Force IPv4
            hostaddr=None,  # Let DNS resolve to IPv4
            options='-c listen_addresses=*'
        )
        logger.info("Database connection pool initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing connection pool: {str(e)}")
        connection_pool = None

def get_db_connection(max_retries: int = 3, retry_delay: int = 1) -> Optional[psycopg2.extensions.connection]:
    """Get a database connection with retry logic"""
    global connection_pool

    if connection_pool is None:
        init_connection_pool()
        if connection_pool is None:
            return None

    for attempt in range(max_retries):
        try:
            conn = connection_pool.getconn()
            if conn:
                return conn
        except Exception as e:
            logger.error(f"Connection attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                continue
            else:
                logger.error("All connection attempts failed")
                return None
    return None

def return_db_connection(conn):
    """Return a connection to the pool"""
    if connection_pool and conn:
        connection_pool.putconn(conn)

def initialize_database():
    """Create the courts table and scraper status table"""
    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to get database connection for initialization")
        return
    cur = conn.cursor()

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
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            maintenance_notice TEXT,
            maintenance_start TIMESTAMP,
            maintenance_end TIMESTAMP
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
    return_db_connection(conn)

def get_court_data():
    """Get all court data from the database"""
    expected_columns = [
        'id', 'name', 'type', 'status', 'lat', 'lon', 
        'address', 'image_url', 'last_updated'
    ]

    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to get database connection")
        return pd.DataFrame(columns=expected_columns)

    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT * FROM courts ORDER BY name")
        data = cur.fetchall()
        if data:
            df = pd.DataFrame(data)
            for col in expected_columns:
                if col not in df.columns:
                    df[col] = None
            return df[expected_columns]
        else:
            return pd.DataFrame(columns=expected_columns)
    except Exception as e:
        logger.error(f"Error getting court data: {str(e)}")
        return pd.DataFrame(columns=expected_columns)
    finally:
        cur.close()
        return_db_connection(conn)

def get_scraper_status():
    """Get the latest scraper status"""
    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to get database connection")
        return {
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
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT * FROM scraper_status 
            ORDER BY start_time DESC 
            LIMIT 1
        """)
        status = cur.fetchone()
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
    except Exception as e:
        logger.error(f"Error getting scraper status: {str(e)}")
        return {
            'status': 'error',
            'message': f'Error getting scraper status: {str(e)}'
        }
    finally:
        cur.close()
        return_db_connection(conn)

def get_scraper_logs(limit=50):
    """Get the most recent scraper logs"""
    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to get database connection")
        return []
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT timestamp, level, message 
            FROM scraper_logs 
            ORDER BY timestamp DESC 
            LIMIT %s
        """, (limit,))
        logs = cur.fetchall()
        return logs
    except Exception as e:
        logger.error(f"Error getting scraper logs: {str(e)}")
        return []
    finally:
        cur.close()
        return_db_connection(conn)

def add_scraper_log(level, message, scraper_run_id=None):
    """Add a new scraper log entry"""
    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to get database connection")
        return
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO scraper_logs (level, message, scraper_run_id)
            VALUES (%s, %s, %s)
        """, (level, message, scraper_run_id))
        conn.commit()
    except Exception as e:
        logger.error(f"Error adding scraper log: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        return_db_connection(conn)

def update_scraper_status(scraper_run_id: int, sources_processed: int, total_sources: int, 
                         status: str, message: str, current_court: str = None, 
                         next_court: str = None, stage: str = None):
    """Updates the status of the scraper run with proper parameter handling."""
    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to get database connection")
        return
    cur = conn.cursor()
    try:
        if status == 'completed':
            cur.execute("""
                UPDATE scraper_status 
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
                UPDATE scraper_status
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
        return_db_connection(conn)

def get_court_types():
    """Get unique court types from the database"""
    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to get database connection")
        return ['Supreme Court', 'Appeals Court', 'District Court', 'Bankruptcy Court', 'Other']
    cur = conn.cursor()
    try:
        cur.execute("SELECT DISTINCT type FROM courts ORDER BY type")
        types = [row[0] for row in cur.fetchall() if row[0] is not None]
        return types if types else ['Supreme Court', 'Appeals Court', 'District Court', 'Bankruptcy Court', 'Other']
    except Exception as e:
        logger.error(f"Error getting court types: {str(e)}")
        return ['Supreme Court', 'Appeals Court', 'District Court', 'Bankruptcy Court', 'Other']
    finally:
        cur.close()
        return_db_connection(conn)

def get_court_statuses() -> list:
    """Get unique court statuses with error handling"""
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            logger.error("Failed to get database connection")
            return ['Open', 'Closed', 'Limited Operations']  # Default fallback values

        cur = conn.cursor()
        cur.execute("SELECT DISTINCT status FROM courts ORDER BY status")
        statuses = [row[0] for row in cur.fetchall() if row[0] is not None]
        cur.close()

        return statuses if statuses else ['Open', 'Closed', 'Limited Operations']
    except Exception as e:
        logger.error(f"Error getting court statuses: {str(e)}")
        return ['Open', 'Closed', 'Limited Operations']  # Default fallback values
    finally:
        if conn:
            return_db_connection(conn)


def log_api_usage(endpoint: str, tokens_used: int, model: str, success: bool, error_message: str = None):
    """Log OpenAI API usage"""
    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to get database connection")
        return
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO api_usage (endpoint, tokens_used, model, success, error_message)
            VALUES (%s, %s, %s, %s, %s)
        """, (endpoint, tokens_used, model, success, error_message))
        conn.commit()
    except Exception as e:
        logger.error(f"Error logging API usage: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        return_db_connection(conn)

def get_api_usage_stats():
    """Get API usage statistics"""
    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to get database connection")
        return {'overall': None, 'by_model': [], 'recent': []}
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
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

        cur.execute("""
            SELECT *
            FROM api_usage
            ORDER BY timestamp DESC
            LIMIT 50
        """)
        recent_calls = cur.fetchall()

        return {
            'overall': overall_stats,
            'by_model': model_stats,
            'recent': recent_calls
        }
    except Exception as e:
        logger.error(f"Error getting API usage stats: {str(e)}")
        return {'overall': None, 'by_model': [], 'recent': []}
    finally:
        cur.close()
        return_db_connection(conn)


def get_filtered_court_data(filters=None):
    """Get court data with optional filters"""
    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to get database connection")
        return pd.DataFrame(columns=[
            'id', 'name', 'type', 'status', 'address', 'lat', 'lon',
            'jurisdiction_name', 'jurisdiction_type', 'parent_jurisdiction',
            'maintenance_notice', 'maintenance_start', 'maintenance_end'
        ])
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        query = """
            SELECT 
                c.id, c.name, c.type, c.status, c.address, c.lat, c.lon,
                j.name as jurisdiction_name, j.type as jurisdiction_type,
                p.name as parent_jurisdiction,
                c.maintenance_notice, c.maintenance_start, c.maintenance_end
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

            if filters.get('has_maintenance'):
                query += " AND c.maintenance_notice IS NOT NULL"

        query += " ORDER BY c.name"

        cur.execute(query, params)
        data = cur.fetchall()

        if data:
            df = pd.DataFrame(data)
            return df
        else:
            return pd.DataFrame(columns=[
                'id', 'name', 'type', 'status', 'address', 'lat', 'lon',
                'jurisdiction_name', 'jurisdiction_type', 'parent_jurisdiction',
                'maintenance_notice', 'maintenance_start', 'maintenance_end'
            ])
    except Exception as e:
        logger.error(f"Error getting filtered court data: {str(e)}")
        return pd.DataFrame(columns=[
            'id', 'name', 'type', 'status', 'address', 'lat', 'lon',
            'jurisdiction_name', 'jurisdiction_type', 'parent_jurisdiction',
            'maintenance_notice', 'maintenance_start', 'maintenance_end'
        ])
    finally:
        cur.close()
        return_db_connection(conn)

# Initialize the database when the module is imported
initialize_database()