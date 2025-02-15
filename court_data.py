import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import os

def get_db_connection():
    """Create a database connection"""
    return psycopg2.connect(os.environ['DATABASE_URL'])

def initialize_database():
    """Create the courts table and insert initial data"""
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
            image_url TEXT NOT NULL
        );
    """)

    # Insert initial data if table is empty
    cur.execute("SELECT COUNT(*) FROM courts")
    if cur.fetchone()[0] == 0:
        initial_data = [
            ('Supreme Court of the United States', 'Supreme Court', 'Open', 38.8897, -77.0089,
             '1 First Street, NE, Washington, DC 20543',
             'https://images.unsplash.com/photo-1564596489416-23196d12d85c'),
            ('US Court of Appeals for the Ninth Circuit', 'Appeals Court', 'Limited Operations', 37.7786, -122.4192,
             '95 7th Street, San Francisco, CA 94103',
             'https://images.unsplash.com/photo-1564595686486-c6e5cbdbe12c'),
            ('New York Southern District Court', 'District Court', 'Open', 40.7128, -74.0060,
             '500 Pearl Street, New York, NY 10007',
             'https://images.unsplash.com/photo-1600786288398-e795cfac80aa'),
            ('California Central District Court', 'District Court', 'Open', 34.0522, -118.2437,
             '350 W 1st Street, Los Angeles, CA 90012',
             'https://images.unsplash.com/photo-1521984692647-a41fed613ec7'),
            ('Texas Northern District Court', 'District Court', 'Limited Operations', 32.7767, -96.7970,
             '1100 Commerce Street, Dallas, TX 75242',
             'https://images.unsplash.com/photo-1685747750264-a4e932005dde')
        ]

        cur.executemany("""
            INSERT INTO courts (name, type, status, lat, lon, address, image_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, initial_data)

    conn.commit()
    cur.close()
    conn.close()

def get_court_data():
    """Get all court data from the database"""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT * FROM courts")
    data = cur.fetchall()

    cur.close()
    conn.close()

    return pd.DataFrame(data)

def get_court_types():
    """Get unique court types from the database"""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT type FROM courts ORDER BY type")
    types = [row[0] for row in cur.fetchall()]

    cur.close()
    conn.close()

    return types

def get_court_statuses():
    """Get unique court statuses from the database"""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT status FROM courts ORDER BY status")
    statuses = [row[0] for row in cur.fetchall()]

    cur.close()
    conn.close()

    return statuses

# Initialize the database when the module is imported
initialize_database()