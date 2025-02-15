import logging
import os
import psycopg2
from typing import List, Dict, Optional
from psycopg2.extras import execute_values

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_county_courts(conn) -> List[Dict]:
    """Get list of county courts with improved error handling"""
    logger.info("Getting county courts list...")

    if conn is None:
        logger.error("No database connection provided")
        return []

    cur = None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT c.id, c.name, c.type, c.status, c.url
            FROM courts c
            JOIN jurisdictions j ON c.jurisdiction_id = j.id
            WHERE j.type = 'county'
            ORDER BY c.name
        """)

        courts = []
        for row in cur.fetchall():
            if row[0] is not None:  # Ensure ID exists
                courts.append({
                    'id': row[0],
                    'name': row[1] if row[1] else 'Unknown',
                    'type': row[2] if row[2] else 'Unknown',
                    'status': row[3] if row[3] else 'Unknown',
                    'url': row[4] if row[4] else None
                })

        logger.info(f"Successfully retrieved {len(courts)} county courts")
        return courts
    except Exception as e:
        logger.error(f"Error getting county courts: {str(e)}")
        return []
    finally:
        if cur:
            try:
                cur.close()
            except Exception as e:
                logger.error(f"Error closing cursor: {str(e)}")

def initialize_county_courts(conn) -> None:
    """Initialize county court records"""
    logger.info("Initializing county courts...")
    cur = conn.cursor()

    try:
        # Get county jurisdictions
        cur.execute("""
            SELECT j.id, j.name, s.name as state_name
            FROM jurisdictions j
            JOIN jurisdictions s ON j.parent_id = s.id
            WHERE j.type = 'county'
            ORDER BY s.name, j.name
        """)
        counties = cur.fetchall()

        for county_id, county_name, state_name in counties:
            court_types = [
                ('Superior Court', 'County Superior Courts'),
                ('Family Court', 'County Family Courts'),
                ('Criminal Court', 'County Criminal Courts'),
                ('Civil Court', 'County Civil Courts'),
                ('Probate Court', 'County Probate Courts'),
                ('Juvenile Court', 'County Juvenile Courts')
            ]

            for court_name, court_type in court_types:
                cur.execute("""
                    INSERT INTO courts (
                        name, type, jurisdiction_id, status,
                        address, image_url
                    ) VALUES (
                        %s, %s, %s, 'Open',
                        %s, 'https://images.unsplash.com/photo-1564595686486-c6e5cbdbe12c'
                    ) ON CONFLICT (name) DO NOTHING
                """, (
                    f"{county_name} {court_name}",
                    court_type,
                    county_id,
                    f"{court_name}, {county_name}, {state_name}"
                ))

        conn.commit()
        logger.info("Successfully initialized county courts")

    except Exception as e:
        logger.error(f"Error initializing county courts: {str(e)}")
        conn.rollback()
        raise
    finally:
        cur.close()

def scrape_county_courts(conn, court_ids: Optional[List[int]] = None) -> List[Dict]:
    """Scrape county court data"""
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT c.id, c.name, c.type, cs.source_url
            FROM courts c
            JOIN jurisdictions j ON c.jurisdiction_id = j.id
            JOIN court_sources cs ON cs.jurisdiction_id = j.id
            WHERE j.type = 'county'
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