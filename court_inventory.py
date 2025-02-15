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

            # Tribal Courts
            (10, "Tribal Courts", 1, "Courts of sovereign tribal nations", None),

            # Administrative Courts
            (11, "Administrative Courts", 2, "Executive branch administrative courts", None)
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
    """Initialize federal and state jurisdictions"""
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

        # Add state jurisdictions
        states = [
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

        state_values = [(state, 'state', federal_id) for state in states]
        execute_values(cur, """
            INSERT INTO jurisdictions (name, type, parent_id)
            VALUES %s
            ON CONFLICT (name) DO UPDATE SET
                type = EXCLUDED.type,
                parent_id = EXCLUDED.parent_id
        """, state_values)

        logger.info(f"Successfully initialized federal jurisdiction and {len(states)} state jurisdictions")
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

        # State court sources - initial set of known directory URLs
        cur.execute("SELECT id, name FROM jurisdictions WHERE type = 'state'")
        states = cur.fetchall()

        state_patterns = [
            ('https://www.{state}courts.gov', 'State Courts Portal'),
            ('https://www.{state}.gov/courts', 'State Government Courts Page'),
            ('https://www.{state}judiciary.gov', 'State Judiciary Website')
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
    courts = []
    # TODO: Implement proper content parsing based on page structure
    # This is a placeholder that should be expanded based on actual page formats
    return courts

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
                INSERT INTO courts (name, type, url, jurisdiction_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (name) DO UPDATE
                SET type = EXCLUDED.type,
                    url = EXCLUDED.url,
                    last_updated = CURRENT_TIMESTAMP
                RETURNING (xmax = 0) as is_insert
            """, (court['name'], court['type'], court['url'], jurisdiction_id))

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

def build_court_inventory() -> List[Dict]:
    """
    Build a comprehensive inventory of all courts in the United States
    This function is primarily for initial database setup.  Subsequent updates should use update_court_inventory()
    """
    logger.info("Building court inventory...")
    try:
        # Initialize basic structure
        initialize_court_types()
        initialize_jurisdictions()
        initialize_court_sources() # Initialize court sources

        logger.info("Initial court inventory build completed. Subsequent updates will be handled automatically.")
        return [] # Return empty list, as this function only does schema setup.

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