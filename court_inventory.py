import json
import trafilatura
from typing import Dict, List, Optional
import psycopg2
from psycopg2.extras import execute_values
import os
import logging

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

def get_federal_court_directory() -> List[Dict]:
    """
    Fetch the directory of federal courts from uscourts.gov
    Returns a list of court information dictionaries
    """
    base_url = "https://www.uscourts.gov/about-federal-courts/court-website-links"
    try:
        downloaded = trafilatura.fetch_url(base_url)
        if not downloaded:
            logger.error(f"Failed to download content from {base_url}")
            return []

        content = trafilatura.extract(downloaded)
        if not content:
            logger.error("Failed to extract content from downloaded page")
            return []

        # Initial hardcoded courts while we implement proper parsing
        federal_courts = [
            {
                "name": "Supreme Court of the United States",
                "url": "https://www.supremecourt.gov",
                "type": "Supreme Court",
                "jurisdiction": "United States"
            },
            {
                "name": "U.S. Court of Appeals for the First Circuit",
                "url": "http://www.ca1.uscourts.gov",
                "type": "Courts of Appeals",
                "jurisdiction": "United States"
            }
        ]

        logger.info(f"Retrieved {len(federal_courts)} federal courts")
        return federal_courts

    except Exception as e:
        logger.error(f"Error fetching federal court directory: {str(e)}")
        return []

def get_state_court_directory(state: str) -> List[Dict]:
    """
    Fetch the directory of courts for a specific state
    Returns a list of court information dictionaries
    """
    # Initial implementation with sample data
    try:
        # Example structure for state courts
        state_courts = [
            {
                "name": f"{state} Supreme Court",
                "url": f"https://www.{state.lower()}courts.gov/supreme",
                "type": "State Supreme Courts",
                "jurisdiction": state
            },
            {
                "name": f"{state} Court of Appeals",
                "url": f"https://www.{state.lower()}courts.gov/appeals",
                "type": "State Appellate Courts",
                "jurisdiction": state
            }
        ]

        logger.info(f"Retrieved {len(state_courts)} courts for {state}")
        return state_courts

    except Exception as e:
        logger.error(f"Error fetching courts for {state}: {str(e)}")
        return []

def build_court_inventory() -> List[Dict]:
    """
    Build a comprehensive inventory of all courts in the United States
    """
    logger.info("Building court inventory...")
    try:
        # Initialize basic structure
        initialize_court_types()
        initialize_jurisdictions()

        # Get federal courts
        federal_courts = get_federal_court_directory()

        # Get state courts
        state_courts = []
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cur = conn.cursor()

        cur.execute("SELECT name FROM jurisdictions WHERE type = 'state'")
        states = [row[0] for row in cur.fetchall()]

        for state in states:
            state_courts.extend(get_state_court_directory(state))

        cur.close()
        conn.close()

        total_courts = federal_courts + state_courts
        logger.info(f"Built inventory with {len(total_courts)} total courts")
        return total_courts

    except Exception as e:
        logger.error(f"Error building court inventory: {str(e)}")
        return []

if __name__ == "__main__":
    try:
        courts = build_court_inventory()
        print(f"Successfully built inventory of {len(courts)} courts")
    except Exception as e:
        print(f"Error building court inventory: {str(e)}")