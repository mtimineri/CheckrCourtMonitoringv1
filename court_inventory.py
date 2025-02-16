"""
Court inventory management module.
Handles discovery and updates of court information.
"""
import json
from typing import Dict, List, Optional, Tuple
import psycopg2
from psycopg2.extras import execute_values
import os
import logging
from datetime import datetime, timedelta
from urllib.parse import urljoin
import re
import requests
from bs4 import BeautifulSoup
from court_ai_discovery import initialize_ai_discovery, search_court_directories, discover_courts_from_content, verify_court_info

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get a database connection from the connection pool"""
    try:
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        conn.autocommit = False  # Explicit transaction management
        return conn
    except Exception as e:
        logger.error(f"Error getting database connection: {str(e)}")
        return None

def update_scraper_status(
    update_id: int,
    sources_processed: int,
    total_sources: int,
    status: str,
    message: str,
    current_source: Optional[str] = None,
    next_source: Optional[str] = None,
    stage: Optional[str] = None
) -> None:
    """Update the status of the current scraper run with enhanced progress tracking"""
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Failed to get database connection for status update")
            return

        cur = conn.cursor()
        try:
            # Calculate completion percentage
            completion_percentage = (sources_processed / total_sources * 100) if total_sources > 0 else 0

            # Format detailed status message
            detailed_message = (
                f"{message}\n"
                f"Progress: {completion_percentage:.1f}% ({sources_processed}/{total_sources} sources)\n"
                f"Current: {current_source or 'Starting...'}"
            )

            # Update database with latest status
            cur.execute("""
                UPDATE inventory_updates
                SET sources_processed = %s,
                    total_sources = %s,
                    status = %s,
                    message = %s,
                    current_source = %s,
                    next_source = %s,
                    stage = %s,
                    completed_at = CASE 
                        WHEN %s IN ('completed', 'error') THEN CURRENT_TIMESTAMP
                        ELSE NULL
                    END
                WHERE id = %s
                RETURNING id;
            """, (
                sources_processed,
                total_sources,
                status,
                detailed_message,
                current_source,
                next_source,
                stage,
                status,
                update_id
            ))

            # Ensure the update was successful
            if cur.fetchone() is None:
                logger.error(f"Failed to update status for run {update_id}")
                conn.rollback()
                return

            conn.commit()
            logger.info(f"Successfully updated scraper status: {detailed_message}")

        except Exception as e:
            logger.error(f"Error updating scraper status: {str(e)}")
            conn.rollback()
        finally:
            cur.close()
    finally:
        if conn:
            conn.close()

def initialize_inventory_run():
    """Initialize a new inventory update run"""
    logger.info("Initializing new inventory update run")
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Failed to get database connection")
            return None

        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO inventory_updates 
                (started_at, status, stage)
                VALUES (CURRENT_TIMESTAMP, 'running', 'Starting inventory update')
                RETURNING id;
            """)
            run_id = cur.fetchone()[0]
            conn.commit()
            logger.info(f"Created new inventory update run with ID: {run_id}")
            return run_id

        except Exception as e:
            logger.error(f"Error initializing inventory run: {str(e)}")
            conn.rollback()
            return None
        finally:
            cur.close()
    finally:
        if conn:
            conn.close()

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
                contact_info JSONB,
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
                end_time TIMESTAMP,
                total_sources INTEGER,
                sources_processed INTEGER DEFAULT 0,
                new_courts_found INTEGER DEFAULT 0,
                courts_updated INTEGER DEFAULT 0,
                status VARCHAR(50) DEFAULT 'running',
                message TEXT,
                current_source TEXT,
                next_source TEXT,
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
            CREATE INDEX IF NOT EXISTS idx_inventory_updates_status ON inventory_updates(status);
        """)

        # Reset any stalled updates
        cur.execute("""
            UPDATE inventory_updates 
            SET status = 'error',
                completed_at = CURRENT_TIMESTAMP,
                end_time = CURRENT_TIMESTAMP,
                message = 'Reset stalled update'
            WHERE status = 'running'
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
    """Initialize known court directory sources with AI assistance"""
    logger.info("Initializing court directory sources...")

    # Initialize AI discovery first
    if not initialize_ai_discovery():
        logger.error("Failed to initialize AI discovery module")
        return

    conn = get_db_connection()
    if not conn:
        logger.error("Failed to get database connection")
        return

    cur = conn.cursor()
    try:
        # Get federal jurisdiction ID
        cur.execute("SELECT id FROM jurisdictions WHERE name = 'United States'")
        result = cur.fetchone()
        if not result:
            logger.error("Federal jurisdiction not found")
            return
        federal_id = result[0]

        # Get AI-generated court directory URLs
        logger.info("Searching for court directory URLs...")
        directory_urls = search_court_directories()

        if not directory_urls:
            logger.warning("No court directory URLs discovered")
            # Add default federal courts website as fallback
            directory_urls = ["https://www.uscourts.gov"]

        # Add discovered sources
        sources_added = 0
        for url in directory_urls:
            try:
                cur.execute("""
                    INSERT INTO court_sources (jurisdiction_id, source_url, is_active)
                    VALUES (%s, %s, true)
                    ON CONFLICT (jurisdiction_id, source_url) DO UPDATE 
                    SET is_active = true, last_checked = CURRENT_TIMESTAMP
                """, (federal_id, url))
                sources_added += 1
                logger.info(f"Added/updated court source: {url}")
            except Exception as e:
                logger.error(f"Error adding court source {url}: {str(e)}")
                continue

        # Add specific state court websites
        state_courts = [
            ('California', 'https://www.courts.ca.gov'),
            ('New York', 'https://www.nycourts.gov'),
            ('Texas', 'https://www.txcourts.gov'),
            ('Florida', 'https://www.flcourts.org'),
            ('Illinois', 'https://www.illinoiscourts.gov')
        ]

        for state_name, url in state_courts:
            try:
                cur.execute("""
                    SELECT id FROM jurisdictions WHERE name = %s AND type = 'state'
                """, (state_name,))
                result = cur.fetchone()
                if result:
                    state_id = result[0]
                    cur.execute("""
                        INSERT INTO court_sources (jurisdiction_id, source_url, is_active)
                        VALUES (%s, %s, true)
                        ON CONFLICT (jurisdiction_id, source_url) DO UPDATE 
                        SET is_active = true, last_checked = CURRENT_TIMESTAMP
                    """, (state_id, url))
                    sources_added += 1
                    logger.info(f"Added/updated state court source for {state_name}: {url}")
            except Exception as e:
                logger.error(f"Error adding state court source for {state_name}: {str(e)}")
                continue

        conn.commit()
        logger.info(f"Successfully initialized {sources_added} court sources")

    except Exception as e:
        logger.error(f"Error initializing court sources: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

def extract_courts_from_page(content: str, base_url: str) -> List[Dict]:
    """Extract court information from page content"""
    try:
        courts = []
        soup = BeautifulSoup(content, 'html.parser')

        # Look for common court naming patterns in text content
        court_patterns = [
            r"(.*?Court\s+of\s+Appeals.*?)",
            r"(.*?District\s+Court.*?)",
            r"(.*?Superior\s+Court.*?)",
            r"(.*?Supreme\s+Court.*?)",
            r"(.*?Circuit\s+Court.*?)",
            r"(.*?County\s+Court.*?)",
            r"(.*?Municipal\s+Court.*?)",
            r"(.*?Bankruptcy\s+Court.*?)",
            r"(.*?Family\s+Court.*?)",
            r"(.*?Juvenile\s+Court.*?)",
            r"(.*?Criminal\s+Court.*?)"
        ]

        # Extract text from paragraphs and headings
        text_elements = soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'a'])
        for element in text_elements:
            text = element.get_text().strip()

            for pattern in court_patterns:
                matches = re.finditer(pattern, text, re.IGNORECASE)
                for match in matches:
                    court_name = match.group(1).strip()

                    # Skip if this court is already found
                    if any(c['name'] == court_name for c in courts):
                        continue

                    # Determine court type based on name
                    court_type = None
                    if 'Appeals' in court_name:
                        court_type = 'Courts of Appeals'
                    elif 'District' in court_name:
                        court_type = 'District Courts'
                    elif 'Bankruptcy' in court_name:
                        court_type = 'Bankruptcy Courts'
                    elif 'Superior' in court_name:
                        court_type = 'County Superior Courts'
                    elif 'Supreme' in court_name:
                        court_type = 'Supreme Court'
                    elif 'Circuit' in court_name:
                        court_type = 'County Circuit Courts'
                    elif 'Family' in court_name:
                        court_type = 'County Family Courts'
                    elif 'Criminal' in court_name:
                        court_type = 'County Criminal Courts'
                    elif 'Municipal' in court_name:
                        court_type = 'Municipal Courts'
                    else:
                        court_type = 'Other Courts'

                    # Extract URL if available
                    court_url = None
                    if element.name == 'a' and element.has_attr('href'):
                        court_url = urljoin(base_url, element['href'])

                    courts.append({
                        'name': court_name,
                        'type': court_type,
                        'url': court_url,
                        'status': 'Open'  # Default status
                    })

        logger.info(f"Found {len(courts)} courts in content from {base_url}")
        return courts

    except Exception as e:
        logger.error(f"Error extracting courts from page: {str(e)}")
        return []

def process_court_source(source_id: int, url: str, jurisdiction_id: int, update_id: int) -> Tuple[int, int]:
    """Process a single court source using AI-powered discovery"""
    logger.info(f"Starting to process source ID {source_id} with URL: {url}")
    try:
        # Use requests instead of trafilatura for more reliable fetching
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        content = response.text

        # Extract courts using AI-powered discovery
        courts = discover_courts_from_content(content, url)
        logger.info(f"AI Discovery retrieved {len(courts)} courts from {url}")

        if not courts:
            logger.warning(f"No courts found at {url}")
            return 0, 0

        conn = get_db_connection()
        if not conn:
            logger.error(f"Failed to get database connection for source {source_id}")
            return 0, 0

        cur = conn.cursor()
        new_courts = 0
        updated_courts = 0

        try:
            for court in courts:
                # Verify court information using AI
                verified_court = verify_court_info(court)
                if not verified_court.get('verified', False):
                    logger.warning(f"Court verification failed for {court.get('name', 'Unknown')}")
                    continue

                cur.execute("""
                    INSERT INTO courts (
                        name, type, url, jurisdiction_id, status, 
                        address, contact_info, last_updated
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (name) DO UPDATE
                    SET type = EXCLUDED.type,
                        url = EXCLUDED.url,
                        status = EXCLUDED.status,
                        address = EXCLUDED.address,
                        contact_info = EXCLUDED.contact_info,
                        last_updated = CURRENT_TIMESTAMP
                    RETURNING (xmax = 0) as is_insert;
                """, (
                    verified_court['name'],
                    verified_court['type'],
                    verified_court.get('url'),
                    jurisdiction_id,
                    verified_court['status'],
                    verified_court.get('address'),
                    json.dumps(verified_court.get('contact_info', {}))
                ))

                is_insert = cur.fetchone()[0]
                if is_insert:
                    new_courts += 1
                    logger.info(f"Added new court: {verified_court['name']}")
                else:
                    updated_courts += 1
                    logger.info(f"Updated existing court: {verified_court['name']}")

            # Update the scraper run status
            cur.execute("""
                UPDATE inventory_updates
                SET new_courts_found = new_courts_found + %s,
                    courts_updated = courts_updated + %s
                WHERE id = %s
            """, (new_courts, updated_courts, update_id))

            # Update source last_checked timestamp
            cur.execute("""
                UPDATE court_sources 
                SET last_checked = CURRENT_TIMESTAMP,
                    last_updated = CASE 
                        WHEN %s > 0 OR %s > 0 THEN CURRENT_TIMESTAMP 
                        ELSE last_updated 
                    END
                WHERE id = %s
            """, (new_courts, updated_courts, source_id))

            conn.commit()
            logger.info(f"Successfully processed source {source_id}: {new_courts} new, {updated_courts} updated")
            return new_courts, updated_courts

        except Exception as e:
            logger.error(f"Error processing courts from {url}: {str(e)}")
            conn.rollback()
            raise
        finally:
            cur.close()
            conn.close()

    except Exception as e:
        logger.error(f"Error processing source {url}: {str(e)}")
        return 0, 0

def update_court_inventory(court_type: str = 'all') -> Dict:
    """Update the court inventory from all active sources"""
    logger.info(f"Starting court inventory update for type: {court_type}")
    update_id = initialize_inventory_run()
    if update_id is None:
        logger.error("Failed to initialize inventory run")
        return {'status': 'error', 'message': 'Failed to initialize inventory run'}

    conn = get_db_connection()
    if not conn:
        logger.error("Failed to get database connection")
        return {'status': 'error', 'message': 'Failed to get database connection'}

    cur = conn.cursor()

    try:
        # Get active sources that need updating based on court type
        if court_type == 'all':
            # Modified query to check conditions
            cur.execute("""
                SELECT cs.id, cs.jurisdiction_id, cs.source_url, j.type, j.name,
                       cs.last_checked, cs.update_frequency
                FROM court_sources cs
                JOIN jurisdictions j ON cs.jurisdiction_id = j.id
                WHERE cs.is_active = true
                  AND (cs.last_checked IS NULL 
                       OR cs.last_checked < CURRENT_TIMESTAMP - COALESCE(cs.update_frequency, INTERVAL '24 hours'));
            """)
            logger.info("Executing query for all court types")
        else:
            cur.execute("""
                SELECT cs.id, cs.jurisdiction_id, cs.source_url, j.type, j.name,
                       cs.last_checked, cs.update_frequency
                FROM court_sources cs
                JOIN jurisdictions j ON cs.jurisdiction_id = j.id
                WHERE cs.is_active = true
                  AND j.type = %s
                  AND (cs.last_checked IS NULL 
                       OR cs.last_checked < CURRENT_TIMESTAMP - COALESCE(cs.update_frequency, INTERVAL '24 hours'));
            """, (court_type,))
            logger.info(f"Executing query for court type: {court_type}")

        sources = cur.fetchall()
        total_sources = len(sources)

        # Log detailed source information
        logger.info(f"Found {total_sources} sources to process")
        if sources:
            for source in sources[:5]:  # Log first 5 sources for debugging
                logger.info(f"Source details: ID={source[0]}, Type={source[3]}, Name={source[4]}, URL={source[2]}")
        else:
            # Log current time and sample source data for debugging
            cur.execute("""
                SELECT COUNT(*), 
                       MIN(last_checked), 
                       MAX(last_checked),
                       COUNT(CASE WHEN is_active = true THEN 1 END)
                FROM court_sources;
            """)
            stats = cur.fetchone()
            logger.info(f"Debug - Source stats: Total={stats[0]}, "
                       f"Earliest check={stats[1]}, Latest check={stats[2]}, "
                       f"Active={stats[3]}")

        if total_sources == 0:
            logger.warning("No sources found to process")
            return {
                'status': 'completed',
                'total_sources': 0,
                'new_courts': 0,
                'updated_courts': 0,
                'court_type': court_type,
                'message': 'No sources found to process'
            }

        total_new_courts = 0
        total_updated_courts = 0

        # Update initial status
        update_scraper_status(
            update_id, 0, total_sources,
            'running',
            f"Processing {court_type} courts" if court_type != 'all' else "Processing all courts",
            stage='Starting inventory update'
        )

        for i, (source_id, jurisdiction_id, url, j_type, j_name, last_checked, update_freq) in enumerate(sources, 1):
            logger.info(f"Processing source {i}/{total_sources}: {url}")

            # Update status with jurisdiction details
            next_source = sources[i-1][4] if i < len(sources) else "Completion"
            update_scraper_status(
                update_id, i, total_sources,
                'running',
                f'Processing {j_type} jurisdiction: {j_name}',
                current_source=j_name,
                next_source=next_source,
                stage=f'Checking {j_type} courts'
            )

            new_courts, updated_courts = process_court_source(source_id, url, jurisdiction_id, update_id)
            total_new_courts += new_courts
            total_updated_courts += updated_courts


        # Update final status
        completion_message = (
            f"Processed {total_sources} sources for {court_type} courts, "
            f"found {total_new_courts} new courts, updated {total_updated_courts} existing courts"
        ) if court_type != 'all' else (
            f"Processed {total_sources} sources, "
            f"found {total_new_courts} new courts, updated {total_updated_courts} existing courts"
        )

        update_scraper_status(
            update_id, total_sources, total_sources,
            'completed', completion_message,
            current_source='Complete',
            stage='Finished'
        )

        return {
            'status': 'completed',
            'total_sources': total_sources,
            'new_courts': total_new_courts,
            'updated_courts': total_updated_courts,
            'court_type': court_type,
            'message': completion_message
        }

    except Exception as e:
        error_message = f"Error updating court inventory: {str(e)}"
        logger.error(error_message)
        if update_id:
            update_scraper_status(
                update_id, 0, total_sources if 'total_sources' in locals() else 0,
                'error', error_message,
                current_source='Error',
                stage='Failed'
            )
        return {
            'status': 'error',
            'message': error_message
        }
    finally:
        cur.close()
        conn.close()

def initialize_base_courts() -> None:
    """Initialize base court records through database"""
    logger.info("Initializing base court records...")
    conn = None
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Failed to get database connection")
            return

        cur = conn.cursor()
        try:
            # Get federal jurisdiction ID
            cur.execute("SELECT id FROM jurisdictions WHERE name = 'United States'")
            federal_id = cur.fetchone()
            if not federal_id:
                logger.error("Federal jurisdiction not found")
                return
            federal_id = federal_id[0]

            # Add Supreme Court through database
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

            # Insert Circuit Courts data through database
            circuits_data = [
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

            # Insert circuit courts using execute_values for better performance
            circuit_values = []
            for circuit, location, lat, lon in circuits_data:
                url = ("https://www.cadc.uscourts.gov" if circuit == "D.C. Circuit"
                      else "https://cafc.uscourts.gov" if circuit == "Federal Circuit"
                      else f"https://www.ca{circuits_data.index((circuit, location, lat, lon)) + 1}.uscourts.gov")

                circuit_values.append((
                    f"U.S. Court of Appeals for the {circuit}",
                    'Courts of Appeals',
                    url,
                    federal_id,
                    'Open',
                    f"Federal Courthouse, {location}",
                    'https://images.unsplash.com/photo-1564595686486-c6e5cbdbe12c',
                    lat,
                    lon
                ))

            execute_values(cur, """
                INSERT INTO courts (
                    name, type, url, jurisdiction_id, status,
                    address, image_url, lat, lon
                ) VALUES %s
                ON CONFLICT (name) DO UPDATE SET
                    url = EXCLUDED.url,
                    status = EXCLUDED.status,
                    address = EXCLUDED.address,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon
            """, circuit_values)

            # Initialize district courts data through database
            district_courts_data = [
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

            # Insert district courts using execute_values
            district_values = []
            for name, location, lat, lon in district_courts_data:
                url = f"https://www.{name.lower().replace(' ', '')}.uscourts.gov"
                district_values.append((
                    f"U.S. District Court for the {name}",
                    'District Courts',
                    url,
                    federal_id,
                    'Open',
                    f"Federal Courthouse, {location}",
                    'https://images.unsplash.com/photo-1564595686486-c6e5cbdbe12c',
                    lat,
                    lon
                ))

            execute_values(cur, """
                INSERT INTO courts (
                    name, type, url, jurisdiction_id, status,
                    address, image_url, lat, lon
                ) VALUES %s
                ON CONFLICT (name) DO UPDATE SET
                    url = EXCLUDED.url,
                    status = EXCLUDED.status,
                    address = EXCLUDED.address,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon
            """, district_values)

            # Add Major Bankruptcy Courts through database
            bankruptcy_courts = [
                ("Southern District of New York", "New York, NY", 40.7143, -74.0060),
                ("District of Delaware", "Wilmington, DE", 39.7447, -75.5484),
                ("Central District of California", "Los Angeles, CA", 34.0522, -118.2437),
                ("Northern District of Illinois", "Chicago, IL", 41.8781, -87.6298),
                ("Southern District of Texas", "Houston, TX", 29.7604, -95.3698)
            ]

            # Insert bankruptcy courts using execute_values
            bankruptcy_values = []
            for district, location, lat, lon in bankruptcy_courts:
                url = f"https://www.{district.lower().replace(' ', '')}.uscourts.gov/bankruptcy"
                bankruptcy_values.append((
                    f"U.S. Bankruptcy Court for the {district}",
                    'Bankruptcy Courts',
                    url,
                    federal_id,
                    'Open',
                    f"Federal Courthouse, {location}",
                    'https://images.unsplash.com/photo-1564595686486-c6e5cbdbe12c',
                    lat,
                    lon
                ))

            execute_values(cur, """
                INSERT INTO courts (
                    name, type, url, jurisdiction_id, status,
                    address, image_url, lat, lon
                ) VALUES %s
                ON CONFLICT (name) DO UPDATE SET
                    url = EXCLUDED.url,
                    status = EXCLUDED.status,
                    address = EXCLUDED.address,
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon
            """, bankruptcy_values)

            # Add County Courts through database
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
    finally:
        if conn:
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

def return_db_connection(conn):
    try:
        conn.close()
    except Exception as e:
        logger.error(f"Error closing database connection: {str(e)}")

def get_db_connection():
    """Get a database connection from the connection pool"""
    try:
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        conn.autocommit = False  # Explicit transaction management
        return conn
    except Exception as e:
        logger.error(f"Error getting database connection: {str(e)}")
        return None

if __name__ == "__main__":
    try:
        courts = build_court_inventory()
        print(f"Successfully built initial inventory. Starting automatic updates...")
        result = update_court_inventory()
        print(f"Inventory update completed: {result}")
    except Exception as e:
        print(f"Error building court inventory or updating: {str(e)}")