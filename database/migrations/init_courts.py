"""
Database migration for initializing court data.
This module handles the initial seeding of court data into the database.
"""

import logging
import os
import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

def get_db_connection():
    """Get a database connection"""
    try:
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        conn.autocommit = False  # Explicit transaction management
        return conn
    except Exception as e:
        logger.error(f"Error getting database connection: {str(e)}")
        return None

def seed_initial_courts():
    """Seed the initial court data into the database"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Failed to get database connection")
            return False

        cur = conn.cursor()
        try:
            # Get federal jurisdiction ID
            cur.execute("SELECT id FROM jurisdictions WHERE name = 'United States'")
            federal_id = cur.fetchone()
            if not federal_id:
                logger.error("Federal jurisdiction not found")
                return False
            federal_id = federal_id[0]

            # Supreme Court Data
            supreme_court_data = {
                'name': 'Supreme Court of the United States',
                'type': 'Supreme Court',
                'url': 'https://www.supremecourt.gov',
                'address': '1 First Street, NE Washington, DC 20543',
                'lat': 38.8897,
                'lon': -77.0044
            }

            # Circuit Courts Data
            circuit_courts_data = [
                ("First Circuit", "Boston, MA", 42.3601, -71.0589),
                ("Second Circuit", "New York, NY", 40.7128, -74.0060),
                ("Third Circuit", "Philadelphia, PA", 39.9526, -75.1652),
                # ... other circuits ...
            ]

            # District Courts Data
            district_courts_data = [
                ("Southern District of New York", "New York, NY", 40.7143, -74.0060),
                ("Central District of California", "Los Angeles, CA", 34.0522, -118.2437),
                # ... other districts ...
            ]

            # Insert Supreme Court
            cur.execute("""
                INSERT INTO courts (name, type, url, jurisdiction_id, status, address, lat, lon)
                VALUES (%(name)s, %(type)s, %(url)s, %s, 'Open', %(address)s, %(lat)s, %(lon)s)
                ON CONFLICT (name) DO UPDATE SET
                    url = EXCLUDED.url,
                    status = EXCLUDED.status,
                    address = EXCLUDED.address,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon
            """, {**supreme_court_data, 'jurisdiction_id': federal_id})

            # Insert Circuit Courts using execute_values
            circuit_values = [
                (f"U.S. Court of Appeals for the {circuit}", 
                 'Courts of Appeals',
                 f"https://www.ca{i+1}.uscourts.gov" if circuit not in ["D.C. Circuit", "Federal Circuit"]
                 else "https://www.cadc.uscourts.gov" if circuit == "D.C. Circuit"
                 else "https://cafc.uscourts.gov",
                 federal_id,
                 'Open',
                 f"Federal Courthouse, {location}",
                 lat,
                 lon)
                for i, (circuit, location, lat, lon) in enumerate(circuit_courts_data)
            ]

            execute_values(cur, """
                INSERT INTO courts (name, type, url, jurisdiction_id, status, address, lat, lon)
                VALUES %s
                ON CONFLICT (name) DO UPDATE SET
                    url = EXCLUDED.url,
                    status = EXCLUDED.status,
                    address = EXCLUDED.address,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon
            """, circuit_values)

            conn.commit()
            logger.info("Successfully seeded initial court data")
            return True

        except Exception as e:
            logger.error(f"Error seeding court data: {str(e)}")
            conn.rollback()
            return False
        finally:
            cur.close()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    seed_initial_courts()
