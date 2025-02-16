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
    """Initialize the database connection pool with proper validation"""
    global connection_pool
    try:
        # Parse DATABASE_URL
        url = urlparse(os.environ['DATABASE_URL'])
        logger.info("Initializing database connection pool...")

        # Create connection pool
        connection_pool = pool.SimpleConnectionPool(
            min_connections,
            max_connections,
            user=url.username,
            password=url.password,
            host=url.hostname,
            port=url.port or 5432,
            database=url.path[1:],  # Remove leading slash
        )

        # Validate pool by testing a connection
        test_conn = connection_pool.getconn()
        if test_conn:
            try:
                cur = test_conn.cursor()
                cur.execute('SELECT 1')
                cur.close()
                logger.info("Database connection pool initialized and validated successfully")
            finally:
                connection_pool.putconn(test_conn)
        return True
    except Exception as e:
        logger.error(f"Error initializing connection pool: {str(e)}")
        connection_pool = None
        return False

def get_db_connection(max_retries: int = 3, retry_delay: int = 1) -> Optional[psycopg2.extensions.connection]:
    """Get a database connection with improved retry logic and validation"""
    global connection_pool

    # Initialize pool if it doesn't exist
    if connection_pool is None:
        if not init_connection_pool():
            logger.error("Failed to initialize connection pool")
            return None

    # Try to get a connection
    for attempt in range(max_retries):
        try:
            conn = connection_pool.getconn()
            if conn and not conn.closed:
                # Test the connection
                cur = conn.cursor()
                cur.execute('SELECT 1')
                cur.close()
                logger.info("Successfully acquired database connection")
                return conn
            else:
                # Return bad connection to pool and retry
                if conn:
                    try:
                        connection_pool.putconn(conn)
                    except Exception as e:
                        logger.error(f"Error returning connection to pool: {str(e)}")
        except Exception as e:
            logger.error(f"Connection attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                continue
            else:
                logger.error("All connection attempts failed")
                return None
    return None

def return_db_connection(conn):
    """Return a connection to the pool"""
    if connection_pool and conn and not conn.closed:
        try:
            connection_pool.putconn(conn)
        except Exception as e:
            logger.error(f"Error returning connection to pool: {str(e)}")
            try:
                conn.close()
            except:
                pass

def initialize_database():
    """Create the courts table and scraper status table"""
    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to get database connection for initialization")
        return
    cur = conn.cursor()

    try:
        # First drop the existing foreign key constraint if it exists
        cur.execute("""
            ALTER TABLE scraper_logs 
            DROP CONSTRAINT IF EXISTS scraper_logs_scraper_run_id_fkey;
        """)

        # Create inventory_updates table for Location Scraper
        cur.execute("""
            CREATE TABLE IF NOT EXISTS inventory_updates (
                id SERIAL PRIMARY KEY,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                sources_processed INTEGER DEFAULT 0,
                total_sources INTEGER,
                status VARCHAR(50) DEFAULT 'running',
                message TEXT,
                current_source TEXT,
                next_source TEXT,
                stage TEXT
            );

            -- Create scraper_status table for Data Scraper
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
                stage TEXT,
                court_type VARCHAR(50)
            );

            -- Create scraper_logs table that can reference both systems
            CREATE TABLE IF NOT EXISTS scraper_logs (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                level VARCHAR(20) NOT NULL,
                message TEXT NOT NULL,
                scraper_run_id INTEGER,
                inventory_run_id INTEGER,
                FOREIGN KEY (scraper_run_id) REFERENCES scraper_status(id),
                FOREIGN KEY (inventory_run_id) REFERENCES inventory_updates(id)
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
        logger.info("Database schema initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        conn.rollback()
    finally:
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

def add_scraper_log(level, message, scraper_run_id=None, inventory_run_id=None):
    """Add a new scraper log entry"""
    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to get database connection")
        return
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO scraper_logs (level, message, scraper_run_id, inventory_run_id)
            VALUES (%s, %s, %s, %s)
        """, (level, message, scraper_run_id, inventory_run_id))
        conn.commit()
    except Exception as e:
        logger.error(f"Error adding scraper log: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        return_db_connection(conn)

def update_scraper_status(scraper_run_id: int, courts_processed: int, total_courts: int, 
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
                SET courts_processed = %s,
                    total_courts = %s,
                    status = %s,
                    message = %s,
                    current_court = %s,
                    next_court = %s,
                    stage = %s,
                    end_time = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (courts_processed, total_courts, status, message, 
                  current_court, next_court, stage, scraper_run_id))
        else:
            cur.execute("""
                UPDATE scraper_status
                SET courts_processed = %s,
                    total_courts = %s,
                    status = %s,
                    message = %s,
                    current_court = %s,
                    next_court = %s,
                    stage = %s
                WHERE id = %s
            """, (courts_processed, total_courts, status, message,
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
        return []  # Return empty list instead of hardcoded values
    cur = conn.cursor()
    try:
        cur.execute("SELECT DISTINCT type FROM courts ORDER BY type")
        types = [row[0] for row in cur.fetchall() if row[0] is not None]
        return types if types else []  # Return empty list if no types found
    except Exception as e:
        logger.error(f"Error getting court types: {str(e)}")
        return []  # Return empty list on error
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
            return []  # Return empty list instead of hardcoded values

        cur = conn.cursor()
        cur.execute("SELECT DISTINCT status FROM courts ORDER BY status")
        statuses = [row[0] for row in cur.fetchall() if row[0] is not None]
        cur.close()

        return statuses if statuses else []  # Return empty list if no statuses found
    except Exception as e:
        logger.error(f"Error getting court statuses: {str(e)}")
        return []  # Return empty list on error
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