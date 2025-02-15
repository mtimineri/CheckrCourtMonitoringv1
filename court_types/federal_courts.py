import logging
import os
import psycopg2
from typing import List, Dict, Optional
from psycopg2.extras import execute_values

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_federal_courts(conn) -> List[Dict]:
    """Get list of federal courts"""
    logger.info("Getting federal courts list...")
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT c.id, c.name, c.type, c.status, c.url
            FROM courts c
            JOIN jurisdictions j ON c.jurisdiction_id = j.id
            WHERE j.type = 'federal'
            ORDER BY c.name
        """)

        courts = [
            {
                'id': row[0],
                'name': row[1],
                'type': row[2],
                'status': row[3],
                'url': row[4]
            }
            for row in cur.fetchall()
        ]

        return courts
    finally:
        cur.close()

def scrape_federal_courts(conn, court_ids: Optional[List[int]] = None) -> List[Dict]:
    """Scrape federal court data"""
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT c.id, c.name, c.type, cs.source_url
            FROM courts c
            JOIN jurisdictions j ON c.jurisdiction_id = j.id
            JOIN court_sources cs ON cs.jurisdiction_id = j.id
            WHERE j.type = 'federal'
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

def initialize_federal_courts(conn) -> None:
    """Initialize federal court records"""
    logger.info("Initializing federal courts...")
    cur = conn.cursor()

    try:
        # Get federal jurisdiction ID
        cur.execute("SELECT id FROM jurisdictions WHERE name = 'United States'")
        federal_id = cur.fetchone()[0]

        # Add Supreme Court
        cur.execute("""
            INSERT INTO courts (
                name, type, url, jurisdiction_id, status, 
                address, image_url, lat, lon
            ) VALUES (
                'Supreme Court of the United States',
                'Supreme Court',
                'https://www.supremecourt.gov',
                %s,
                'Open',
                '1 First Street, NE Washington, DC 20543',
                'https://images.unsplash.com/photo-1564596489416-23196d12d85c',
                38.8897,
                -77.0044
            ) ON CONFLICT (name) DO UPDATE SET
                url = EXCLUDED.url,
                status = EXCLUDED.status,
                address = EXCLUDED.address,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon
        """, (federal_id,))

        # Add Circuit Courts
        circuits = [
            ("First Circuit", "Boston, MA", 42.3601, -71.0589),
            ("Second Circuit", "New York, NY", 40.7128, -74.0060),
            ("Third Circuit", "Philadelphia, PA", 39.9526, -75.1652),
            ("Fourth Circuit", "Richmond, VA", 37.5407, -77.4360),
            ("Fifth Circuit", "New Orleans, LA", 29.9511, -90.0715),
            ("Sixth Circuit", "Cincinnati, OH", 39.1031, -84.5120),
            ("Seventh Circuit", "Chicago, IL", 41.8781, -87.6298),
            ("Eighth Circuit", "St. Louis, MO", 38.6270, -90.1994),
            ("Ninth Circuit", "San Francisco, CA", 37.7749, -122.4194),
            ("Tenth Circuit", "Denver, CO", 39.7392, -104.9903),
            ("Eleventh Circuit", "Atlanta, GA", 33.7490, -84.3880),
            ("D.C. Circuit", "Washington, DC", 38.8977, -77.0365),
            ("Federal Circuit", "Washington, DC", 38.8977, -77.0365)
        ]

        for circuit, location, lat, lon in circuits:
            # Generate URL format based on circuit name
            if circuit == "D.C. Circuit":
                url = "https://www.cadc.uscourts.gov"
            elif circuit == "Federal Circuit":
                url = "https://cafc.uscourts.gov"
            else:
                circuit_num = str(circuits.index((circuit, location, lat, lon)) + 1)
                url = f"https://www.ca{circuit_num}.uscourts.gov"

            cur.execute("""
                INSERT INTO courts (
                    name, type, url, jurisdiction_id, status,
                    address, image_url, lat, lon
                ) VALUES (
                    %s,
                    'Courts of Appeals',
                    %s,
                    %s,
                    'Open',
                    %s,
                    'https://images.unsplash.com/photo-1564595686486-c6e5cbdbe12c',
                    %s,
                    %s
                ) ON CONFLICT (name) DO UPDATE SET
                    url = EXCLUDED.url,
                    status = EXCLUDED.status,
                    address = EXCLUDED.address,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon
            """, (
                f"U.S. Court of Appeals for the {circuit}",
                url,
                federal_id,
                f"Federal Courthouse, {location}",
                lat,
                lon
            ))

        conn.commit()
        logger.info("Successfully initialized federal courts")

    except Exception as e:
        logger.error(f"Error initializing federal courts: {str(e)}")
        conn.rollback()
        raise
    finally:
        cur.close()