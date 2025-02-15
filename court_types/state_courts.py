import logging
import os
import psycopg2
from typing import List, Dict, Optional
from psycopg2.extras import execute_values

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_state_courts(conn) -> None:
    """Initialize state court records"""
    logger.info("Initializing state courts...")
    cur = conn.cursor()

    try:
        # Get state jurisdictions
        cur.execute("""
            SELECT id, name 
            FROM jurisdictions 
            WHERE type = 'state'
        """)
        states = cur.fetchall()

        for state_id, state_name in states:
            # Add Supreme Court
            cur.execute("""
                INSERT INTO courts (
                    name, type, jurisdiction_id, status,
                    address, image_url
                ) VALUES (
                    %s, 'State Supreme Courts', %s, 'Open',
                    %s, 'https://images.unsplash.com/photo-1564595686486-c6e5cbdbe12c'
                ) ON CONFLICT (name) DO NOTHING
            """, (
                f"{state_name} Supreme Court",
                state_id,
                f"State Capitol Building, {state_name}"
            ))

            # Add Court of Appeals
            cur.execute("""
                INSERT INTO courts (
                    name, type, jurisdiction_id, status,
                    address, image_url
                ) VALUES (
                    %s, 'State Appellate Courts', %s, 'Open',
                    %s, 'https://images.unsplash.com/photo-1564595686486-c6e5cbdbe12c'
                ) ON CONFLICT (name) DO NOTHING
            """, (
                f"{state_name} Court of Appeals",
                state_id,
                f"State Judicial Center, {state_name}"
            ))

        conn.commit()
        logger.info("Successfully initialized state courts")

    except Exception as e:
        logger.error(f"Error initializing state courts: {str(e)}")
        conn.rollback()
        raise
    finally:
        cur.close()

def scrape_state_courts(conn, court_ids: Optional[List[int]] = None) -> List[Dict]:
    """Scrape state court data"""
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT c.id, c.name, c.type, cs.source_url
            FROM courts c
            JOIN jurisdictions j ON c.jurisdiction_id = j.id
            JOIN court_sources cs ON cs.jurisdiction_id = j.id
            WHERE j.type = 'state'
            AND cs.is_active = true
            AND (%s IS NULL OR c.id = ANY(%s))
            ORDER BY c.name
        """, (court_ids, court_ids))
        
        courts = [
            {
                'id': row[0],
                'name': row[1],
                'type': row[2],
                'url': row[3]
            }
            for row in cur.fetchall()
        ]
        
        return courts
    finally:
        cur.close()
