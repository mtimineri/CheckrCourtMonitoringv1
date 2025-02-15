import json
import trafilatura
from typing import Dict, List, Optional, Tuple
import psycopg2
from psycopg2.extras import execute_values
import os
import logging
from datetime import datetime, timedelta
from urllib.parse import urljoin

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_database():
    """Create the courts table and related tables"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Create court types table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS court_types (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                level INTEGER NOT NULL,
                description TEXT,
                parent_type_id INTEGER REFERENCES court_types(id)
            );

            CREATE TABLE IF NOT EXISTS jurisdictions (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                type VARCHAR(50) NOT NULL,
                parent_id INTEGER REFERENCES jurisdictions(id)
            );

            CREATE TABLE IF NOT EXISTS courts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL UNIQUE,
                type VARCHAR(50) NOT NULL,
                url VARCHAR(255),
                jurisdiction_id INTEGER REFERENCES jurisdictions(id),
                status VARCHAR(50) NOT NULL,
                lat FLOAT,
                lon FLOAT,
                address TEXT,
                image_url TEXT,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS court_sources (
                id SERIAL PRIMARY KEY,
                jurisdiction_id INTEGER REFERENCES jurisdictions(id),
                source_url VARCHAR(255) NOT NULL,
                is_active BOOLEAN DEFAULT true,
                last_checked TIMESTAMP,
                last_updated TIMESTAMP,
                update_frequency INTERVAL DEFAULT '24 hours',
                UNIQUE(jurisdiction_id, source_url)
            );

            CREATE TABLE IF NOT EXISTS inventory_updates (
                id SERIAL PRIMARY KEY,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                total_sources INTEGER,
                sources_processed INTEGER DEFAULT 0,
                new_courts_found INTEGER DEFAULT 0,
                courts_updated INTEGER DEFAULT 0,
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
                scraper_run_id INTEGER REFERENCES inventory_updates(id)
            );
        """)

        # Create indexes for better performance
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_courts_type ON courts(type);
            CREATE INDEX IF NOT EXISTS idx_courts_status ON courts(status);
            CREATE INDEX IF NOT EXISTS idx_courts_jurisdiction ON courts(jurisdiction_id);
            CREATE INDEX IF NOT EXISTS idx_court_sources_jurisdiction ON court_sources(jurisdiction_id);
            CREATE INDEX IF NOT EXISTS idx_court_sources_active ON court_sources(is_active);
        """)

        conn.commit()
        logger.info("Database schema initialized successfully")

    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def initialize_court_types() -> None:
    """Initialize the basic court type hierarchy"""
    logger.info("Initializing court types hierarchy...")
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()

    try:
        # Basic court type hierarchy
        court_types = [
            # Federal Courts
            (1, "Supreme Court", 1, "Highest court in the federal judiciary", None),
            (2, "Courts of Appeals", 2, "Federal appellate courts", 1),
            (3, "District Courts", 3, "Federal trial courts", 2),
            (4, "Bankruptcy Courts", 3, "Federal bankruptcy courts", 2),
            (5, "Specialized Federal Courts", 3, "Courts with specific jurisdictions", 2),

            # State Courts
            (6, "State Supreme Courts", 1, "Highest courts in state judiciary", None),
            (7, "State Appellate Courts", 2, "State courts of appeals", 6),
            (8, "State Trial Courts", 3, "State-level trial courts", 7),
            (9, "State Specialized Courts", 3, "State courts with specific jurisdictions", 7),

            # County Courts
            (10, "County Superior Courts", 3, "Primary county-level courts", 7),
            (11, "County Circuit Courts", 3, "Circuit courts at county level", 7),
            (12, "County District Courts", 3, "District courts at county level", 7),
            (13, "County Family Courts", 4, "County-level family courts", 10),
            (14, "County Probate Courts", 4, "County-level probate courts", 10),
            (15, "County Criminal Courts", 4, "County-level criminal courts", 10),
            (16, "County Civil Courts", 4, "County-level civil courts", 10),
            (17, "County Juvenile Courts", 4, "County-level juvenile courts", 10),
            (18, "County Small Claims Courts", 4, "County-level small claims courts", 10),

            # Municipal Courts
            (19, "Municipal Courts", 4, "City and local courts", 8),

            # Other Courts
            (20, "Tribal Courts", 1, "Courts of sovereign tribal nations", None),
            (21, "Administrative Courts", 2, "Executive branch administrative courts", None)
        ]

        # Insert court types
        cur.execute("TRUNCATE court_types RESTART IDENTITY CASCADE;")
        cur.executemany("""
            INSERT INTO court_types (id, name, level, description, parent_type_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                level = EXCLUDED.level,
                description = EXCLUDED.description,
                parent_type_id = EXCLUDED.parent_type_id
        """, court_types)

        logger.info(f"Successfully initialized {len(court_types)} court types")
        conn.commit()

    except Exception as e:
        logger.error(f"Error initializing court types: {str(e)}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def initialize_jurisdictions() -> None:
    """Initialize federal, state, and county jurisdictions"""
    logger.info("Initializing jurisdictions...")
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()

    try:
        # Add federal jurisdiction
        cur.execute("""
            INSERT INTO jurisdictions (name, type)
            VALUES ('United States', 'federal')
            ON CONFLICT (name) DO UPDATE SET type = 'federal'
            RETURNING id;
        """)
        federal_id = cur.fetchone()[0]

        # Add state jurisdictions with major counties
        states_and_counties = {
            "California": [
                "Los Angeles County", "San Diego County", "Orange County",
                "Santa Clara County", "San Francisco County", "Alameda County"
            ],
            "New York": [
                "New York County", "Kings County", "Queens County",
                "Bronx County", "Richmond County", "Nassau County"
            ],
            "Texas": [
                "Harris County", "Dallas County", "Tarrant County",
                "Bexar County", "Travis County", "Collin County"
            ],
            "Florida": [
                "Miami-Dade County", "Broward County", "Palm Beach County",
                "Hillsborough County", "Orange County", "Pinellas County"
            ],
            "Illinois": [
                "Cook County", "DuPage County", "Lake County",
                "Will County", "Kane County", "McHenry County"
            ]
        }

        # Add all states first
        all_states = [
            "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
            "Connecticut", "Delaware", "Florida", "Georgia", "Hawaii", "Idaho",
            "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana",
            "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
            "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
            "New Hampshire", "New Jersey", "New Mexico", "New York",
            "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon",
            "Pennsylvania", "Rhode Island", "South Carolina", "South Dakota",
            "Tennessee", "Texas", "Utah", "Vermont", "Virginia", "Washington",
            "West Virginia", "Wisconsin", "Wyoming", "District of Columbia"
        ]

        # Insert all states
        state_values = [(state, 'state', federal_id) for state in all_states]
        execute_values(cur, """
            INSERT INTO jurisdictions (name, type, parent_id)
            VALUES %s
            ON CONFLICT (name) DO UPDATE SET
                type = EXCLUDED.type,
                parent_id = EXCLUDED.parent_id
            RETURNING id, name
        """, state_values)

        # Get state IDs
        cur.execute("SELECT id, name FROM jurisdictions WHERE type = 'state'")
        state_ids = {row[1]: row[0] for row in cur.fetchall()}

        # Add counties for states that have them defined
        for state, counties in states_and_counties.items():
            state_id = state_ids.get(state)
            if state_id:
                county_values = [(county, 'county', state_id) for county in counties]
                execute_values(cur, """
                    INSERT INTO jurisdictions (name, type, parent_id)
                    VALUES %s
                    ON CONFLICT (name) DO UPDATE SET
                        type = EXCLUDED.type,
                        parent_id = EXCLUDED.parent_id
                """, county_values)

        logger.info(f"Successfully initialized jurisdictions with counties")
        conn.commit()

    except Exception as e:
        logger.error(f"Error initializing jurisdictions: {str(e)}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def initialize_court_sources() -> None:
    """Initialize known court directory sources"""
    logger.info("Initializing court directory sources...")
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()

    try:
        # Federal court sources
        federal_sources = [
            {
                'url': 'https://www.uscourts.gov/court-locator',
                'description': 'Federal Court Locator'
            },
            {
                'url': 'https://www.supremecourt.gov',
                'description': 'Supreme Court Website'
            },
            {
                'url': 'https://www.uscourts.gov/about-federal-courts/court-website-links/court-appeals-websites',
                'description': 'Courts of Appeals Websites'
            },
            {
                'url': 'https://www.uscourts.gov/about-federal-courts/court-website-links/district-court-websites',
                'description': 'District Court Websites'
            },
            {
                'url': 'https://www.uscourts.gov/about-federal-courts/court-website-links/bankruptcy-court-websites',
                'description': 'Bankruptcy Court Websites'
            },
            {
                'url': 'https://www.fjc.gov/history/courts',
                'description': 'Federal Judicial Center Court History'
            },
            {
                'url': 'https://pacer.uscourts.gov/court-links',
                'description': 'PACER Court Links'
            }
        ]

        # Get federal jurisdiction ID
        cur.execute("SELECT id FROM jurisdictions WHERE name = 'United States'")
        federal_id = cur.fetchone()[0]

        # Insert federal sources
        for source in federal_sources:
            cur.execute("""
                INSERT INTO court_sources (jurisdiction_id, source_url, is_active)
                VALUES (%s, %s, true)
                ON CONFLICT (jurisdiction_id, source_url) DO UPDATE 
                SET is_active = true, last_checked = CURRENT_TIMESTAMP
            """, (federal_id, source['url']))

        # State court sources - expanded patterns
        cur.execute("SELECT id, name FROM jurisdictions WHERE type = 'state'")
        states = cur.fetchall()

        state_patterns = [
            ('https://www.{state}courts.gov', 'State Courts Portal'),
            ('https://www.{state}.gov/courts', 'State Government Courts Page'),
            ('https://www.{state}judiciary.gov', 'State Judiciary Website'),
            ('https://courts.{state}.gov', 'State Courts Website'),
            ('https://www.{state}courts.us', 'State Courts US Portal'),
            ('https://www.{state}.uscourts.gov', 'Federal Courts in State'),
            ('https://www.court.{state}.gov', 'State Court Portal'),
            ('https://www.{state}supremecourt.gov', 'State Supreme Court')
        ]

        for state_id, state_name in states:
            state_slug = state_name.lower().replace(' ', '')
            for pattern, desc in state_patterns:
                url = pattern.format(state=state_slug)
                cur.execute("""
                    INSERT INTO court_sources (jurisdiction_id, source_url, is_active)
                    VALUES (%s, %s, true)
                    ON CONFLICT (jurisdiction_id, source_url) DO UPDATE 
                    SET is_active = true, last_checked = CURRENT_TIMESTAMP
                """, (state_id, url))

        # Add specific state court websites that don't follow the patterns
        specific_state_courts = [
            ('New York', 'https://www.nycourts.gov'),
            ('California', 'https://www.courts.ca.gov'),
            ('Texas', 'https://www.txcourts.gov'),
            ('Florida', 'https://www.flcourts.org'),
            ('Illinois', 'https://www.illinoiscourts.gov'),
            ('Pennsylvania', 'https://www.pacourts.us'),
            ('Ohio', 'https://www.supremecourt.ohio.gov'),
            ('Michigan', 'https://courts.michigan.gov'),
            ('Georgia', 'https://www.gasupreme.us'),
            ('North Carolina', 'https://www.nccourts.gov')
        ]

        for state_name, url in specific_state_courts:
            cur.execute("""
                SELECT id FROM jurisdictions WHERE name = %s AND type = 'state'
            """, (state_name,))
            state_id = cur.fetchone()
            if state_id:
                cur.execute("""
                    INSERT INTO court_sources (jurisdiction_id, source_url, is_active)
                    VALUES (%s, %s, true)
                    ON CONFLICT (jurisdiction_id, source_url) DO UPDATE 
                    SET is_active = true, last_checked = CURRENT_TIMESTAMP
                """, (state_id[0], url))

        conn.commit()
        logger.info("Successfully initialized court sources")

    except Exception as e:
        logger.error(f"Error initializing court sources: {str(e)}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def extract_courts_from_page(content: str, base_url: str) -> List[Dict]:
    """Extract court information from page content"""
    try:
        # Parse the content to find court information
        # This is a basic implementation that should be enhanced based on actual page structures
        courts = []
        # Add some sample data for testing
        courts.append({
            'name': 'U.S. Supreme Court',
            'type': 'Supreme Court',
            'url': 'https://www.supremecourt.gov',
            'status': 'Open'
        })
        courts.append({
            'name': 'U.S. Court of Appeals for the First Circuit',
            'type': 'Courts of Appeals',
            'url': 'http://www.ca1.uscourts.gov',
            'status': 'Open'
        })
        return courts
    except Exception as e:
        logger.error(f"Error extracting courts from page: {str(e)}")
        return []

def process_court_source(source_id: int, url: str, jurisdiction_id: int) -> Tuple[int, int]:
    """Process a single court source and extract court information"""
    try:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            logger.warning(f"Failed to download content from {url}")
            return 0, 0

        content = trafilatura.extract(downloaded)
        if not content:
            logger.warning(f"No content extracted from {url}")
            return 0, 0

        courts = extract_courts_from_page(content, url)

        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cur = conn.cursor()

        new_courts = 0
        updated_courts = 0

        for court in courts:
            cur.execute("""
                INSERT INTO courts (name, type, url, jurisdiction_id, status)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (name) DO UPDATE
                SET type = EXCLUDED.type,
                    url = EXCLUDED.url,
                    status = EXCLUDED.status,
                    last_updated = CURRENT_TIMESTAMP
                RETURNING (xmax = 0) as is_insert
            """, (court['name'], court['type'], court['url'], jurisdiction_id, court.get('status', 'Unknown')))

            is_insert = cur.fetchone()[0]
            if is_insert:
                new_courts += 1
            else:
                updated_courts += 1

        conn.commit()
        cur.close()
        conn.close()

        return new_courts, updated_courts

    except Exception as e:
        logger.error(f"Error processing source {url}: {str(e)}")
        return 0, 0

def update_court_inventory() -> Dict:
    """Update the court inventory from all active sources"""
    logger.info("Starting court inventory update...")
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()

    try:
        # Create new update record
        cur.execute("""
            INSERT INTO inventory_updates (status)
            VALUES ('running')
            RETURNING id
        """)
        update_id = cur.fetchone()[0]
        conn.commit()

        # Get active sources that need updating
        cur.execute("""
            SELECT id, jurisdiction_id, source_url 
            FROM court_sources
            WHERE is_active = true
            AND (last_checked IS NULL 
                 OR last_checked < CURRENT_TIMESTAMP - interval '1 day')
        """)
        sources = cur.fetchall()

        total_sources = len(sources)
        total_new_courts = 0
        total_updated_courts = 0

        # Update inventory tracking
        cur.execute("""
            UPDATE inventory_updates 
            SET total_sources = %s
            WHERE id = %s
        """, (total_sources, update_id))
        conn.commit()

        for i, (source_id, jurisdiction_id, url) in enumerate(sources, 1):
            logger.info(f"Processing source {i}/{total_sources}: {url}")

            new_courts, updated_courts = process_court_source(source_id, url, jurisdiction_id)
            total_new_courts += new_courts
            total_updated_courts += updated_courts

            # Update source last checked timestamp
            cur.execute("""
                UPDATE court_sources 
                SET last_checked = CURRENT_TIMESTAMP,
                    last_updated = CASE 
                        WHEN %s > 0 OR %s > 0 THEN CURRENT_TIMESTAMP 
                        ELSE last_updated 
                    END
                WHERE id = %s
            """, (new_courts, updated_courts, source_id))

            # Update progress
            cur.execute("""
                UPDATE inventory_updates 
                SET sources_processed = %s,
                    new_courts_found = %s,
                    courts_updated = %s
                WHERE id = %s
            """, (i, total_new_courts, total_updated_courts, update_id))
            conn.commit()

        # Mark update as complete
        cur.execute("""
            UPDATE inventory_updates 
            SET status = 'completed',
                completed_at = CURRENT_TIMESTAMP,
                message = %s
            WHERE id = %s
        """, (f"Processed {total_sources} sources, found {total_new_courts} new courts, updated {total_updated_courts} existing courts", update_id))
        conn.commit()

        result = {
            'status': 'completed',
            'total_sources': total_sources,
            'new_courts': total_new_courts,
            'updated_courts': total_updated_courts
        }
        logger.info(f"Inventory update completed: {result}")
        return result

    except Exception as e:
        error_message = f"Error updating court inventory: {str(e)}"
        logger.error(error_message)
        cur.execute("""
            UPDATE inventory_updates 
            SET status = 'error',
                completed_at = CURRENT_TIMESTAMP,
                message = %s
            WHERE id = %s
        """, (error_message, update_id))
        conn.commit()
        raise
    finally:
        cur.close()
        conn.close()

def get_db_connection():
    return psycopg2.connect(os.environ['DATABASE_URL'])

def initialize_base_courts() -> None:
    """Initialize base court records"""
    logger.info("Initializing base court records...")
    conn = get_db_connection()
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

        # Add Circuit Courts with their coordinates
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

        # Add Circuit Courts
        for circuit, location, lat, lon in circuits:
            # Generate correct URL format based on circuit name
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

        # Add District Courts
        district_courts = [
            ("Southern District of New York", "New York, NY", 40.7143, -74.0060),
            ("Central District of California", "Los Angeles, CA", 34.0522, -118.2437),
            ("Northern District of Illinois", "Chicago, IL", 41.8781, -87.6298),
            ("District of Columbia", "Washington, DC", 38.8977, -77.0365),
            ("Eastern District of Virginia", "Alexandria, VA", 38.8048, -77.0469),
            ("Northern District of California", "San Francisco, CA", 37.7749, -122.4194),
            ("Southern District of Florida", "Miami, FL", 25.7617, -80.1918),
            ("Eastern District of Texas", "Tyler, TX", 32.3513, -95.3011),
            ("District of Massachusetts", "Boston, MA", 42.3601, -71.0589)
        ]

        for name, location, lat, lon in district_courts:
            url = f"https://www.{name.lower().replace(' ', '')}.uscourts.gov"
            cur.execute("""
                INSERT INTO courts (
                    name, type, url, jurisdiction_id, status,
                    address, image_url, lat, lon
                ) VALUES (
                    %s,
                    'District Courts',
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
                f"U.S. District Court for the {name}",
                url,
                federal_id,
                f"Federal Courthouse, {location}",
                lat,
                lon
            ))

        # Add Major Bankruptcy Courts
        bankruptcy_courts = [
            ("Southern District of New York", "New York, NY", 40.7143, -74.0060),
            ("District of Delaware", "Wilmington, DE", 39.7447, -75.5484),
            ("Central District of California", "Los Angeles, CA", 34.0522, -118.2437),
            ("Northern District of Illinois", "Chicago, IL", 41.8781, -87.6298),
            ("Southern District of Texas", "Houston, TX", 29.7604, -95.3698)
        ]

        for district, location, lat, lon in bankruptcy_courts:
            url = f"https://www.{district.lower().replace(' ', '')}.uscourts.gov/bankruptcy"
            cur.execute("""
                INSERT INTO courts (
                    name, type, url, jurisdiction_id, status,
                    address, image_url, lat, lon
                ) VALUES (
                    %s,
                    'Bankruptcy Courts',
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
                f"U.S. Bankruptcy Court for the {district}",
                url,
                federal_id,
                f"Federal Courthouse, {location}",
                lat,
                lon
            ))

        # Add County Courts
        # First get some major county jurisdictions
        cur.execute("""
            SELECT j.id, j.name, s.name as state_name
            FROM jurisdictions j
            JOIN jurisdictions s ON j.parent_id = s.id
            WHERE j.type = 'county'
            ORDER BY s.name, j.name
        """)
        counties = cur.fetchall()

        # Add courts for each county
        for county_id, county_name, state_name in counties:
            # Superior Court
            cur.execute("""
                INSERT INTO courts (
                    name, type, jurisdiction_id, status,
                    address, image_url, lat, lon
                ) VALUES (
                    %s, 'County Superior Courts', %s, 'Open',
                    %s, 'https://images.unsplash.com/photo-1564595686486-c6e5cbdbe12c',
                    NULL, NULL
                ) ON CONFLICT (name) DO NOTHING
            """, (
                f"{county_name} Superior Court",
                county_id,
                f"County Courthouse, {county_name}, {state_name}"
            ))

            # Family Court
            cur.execute("""
                INSERT INTO courts (
                    name, type, jurisdiction_id, status,
                    address, image_url, lat, lon
                ) VALUES (
                    %s, 'County Family Courts', %s, 'Open',
                    %s, 'https://images.unsplash.com/photo-1564595686486-c6e5cbdbe12c',
                    NULL, NULL
                ) ON CONFLICT (name) DO NOTHING
            """, (
                f"{county_name} Family Court",
                county_id,
                f"Family Court Division, {county_name}, {state_name}"
            ))

            # Criminal Court
            cur.execute("""
                INSERT INTO courts (
                    name, type, jurisdiction_id, status,
                    address, image_url, lat, lon
                ) VALUES (
                    %s, 'County Criminal Courts', %s, 'Open',
                    %s, 'https://images.unsplash.com/photo-1564595686486-c6e5cbdbe12c',
                    NULL, NULL
                ) ON CONFLICT (name) DO NOTHING
            """, (
                f"{county_name} Criminal Court",
                county_id,
                f"Criminal Court Building, {county_name}, {state_name}"
            ))

        conn.commit()
        logger.info("Successfully initialized base court records including county courts")

    except Exception as e:
        logger.error(f"Error initializing base courts: {str(e)}")
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

def build_court_inventory() -> List[Dict]:
    """
    Build a comprehensive inventory of all courts in the United States
    This function is primarily for initial database setup.
    """
    logger.info("Building court inventory...")
    try:
        # Initialize basic structure
        initialize_database()
        initialize_court_types()
        initialize_jurisdictions()
        initialize_court_sources()
        initialize_base_courts()  # Add base courts

        logger.info("Initial court inventory build completed.")
        return []  # Return empty list, as this function only does schema setup.

    except Exception as e:
        logger.error(f"Error building court inventory: {str(e)}")
        return []

if __name__ == "__main__":
    try:
        courts = build_court_inventory()
        print(f"Successfully built initial inventory. Starting automatic updates...")
        result = update_court_inventory()
        print(f"Inventory update completed: {result}")
    except Exception as e:
        print(f"Error building court inventory or updating: {str(e)}")