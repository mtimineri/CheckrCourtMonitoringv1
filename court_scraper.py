import trafilatura
import pandas as pd
import json
from openai import OpenAI
import os
import time
import logging
from typing import List, Dict, Optional
from court_data import update_scraper_status, add_scraper_log, log_api_usage
from datetime import datetime
from court_types import federal_courts, state_courts, county_courts

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_court_data_from_url(url: str) -> Optional[str]:
    """Fetch and extract text content from a URL"""
    try:
        logger.info(f"Fetching content from URL: {url}")
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            logger.warning(f"No content downloaded from {url}")
            return None
        content = trafilatura.extract(downloaded)
        if content:
            logger.info(f"Successfully extracted content from {url}")
        else:
            logger.warning(f"No content extracted from {url}")
        return content
    except Exception as e:
        logger.error(f"Error fetching URL {url}: {str(e)}")
        return None

def process_court_data(text: str, court_info: Dict) -> Optional[Dict]:
    """Process court data using OpenAI to extract structured information"""
    try:
        logger.info(f"Processing court data for {court_info['name']}")
        client = OpenAI()

        system_prompt = f"""You are a court data extraction expert. Extract court information from the provided text and format it as a JSON object with these fields:
        - name: {court_info['name']} (use this exact name)
        - type: {court_info['type']} (use this exact type)
        - status: one of [Open, Closed, Limited Operations]
        - address: full address
        - lat: latitude as float
        - lon: longitude as float

        Focus on finding the current operational status and location information.
        Use the provided name and type exactly as given.
        Make educated guesses for missing fields based on context."""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": text}
            ],
            response_format={"type": "json_object"}
        )

        # Calculate tokens used
        tokens_used = len(text.split()) + len(system_prompt.split())

        # Extract JSON from the response
        content = response.choices[0].message.content
        result = json.loads(content)

        logger.info(f"Successfully processed data for {court_info['name']}")

        # Log successful API usage
        log_api_usage(
            endpoint="chat.completions",
            tokens_used=tokens_used,
            model="gpt-4o",
            success=True
        )

        return result

    except Exception as e:
        logger.error(f"Error processing court data for {court_info['name']}: {str(e)}")
        # Log failed API usage
        log_api_usage(
            endpoint="chat.completions",
            tokens_used=0,
            model="gpt-4o",
            success=False,
            error_message=str(e)
        )
        return None

def get_courts_to_scrape(court_type: str, court_ids: Optional[List[int]] = None) -> List[Dict]:
    """Get courts to scrape based on type"""
    conn = None
    try:
        import psycopg2
        logger.info(f"Connecting to database to fetch {court_type} courts")
        conn = psycopg2.connect(os.environ['DATABASE_URL'])

        if court_type == 'federal':
            return federal_courts.scrape_federal_courts(conn, court_ids)
        elif court_type == 'state':
            return state_courts.scrape_state_courts(conn, court_ids)
        elif court_type == 'county':
            return county_courts.scrape_county_courts(conn, court_ids)
        else:
            logger.error(f"Unknown court type: {court_type}")
            return []

    except Exception as e:
        logger.error(f"Error getting courts to scrape: {str(e)}")
        return []
    finally:
        if conn:
            conn.close()

def scrape_courts(court_ids: Optional[List[int]] = None, court_type: str = 'all') -> List[Dict]:
    """Scrape court data from their websites"""
    try:
        courts_data = []
        scraper_run_id = None

        # Determine which court types to scrape
        court_types = ['federal', 'state', 'county'] if court_type == 'all' else [court_type]

        total_courts = 0
        courts_processed = 0

        # Get total number of courts to scrape
        for ct in court_types:
            courts = get_courts_to_scrape(ct, court_ids)
            total_courts += len(courts)

        # Start scraping status
        if total_courts > 0:
            scraper_run_id = update_scraper_status(
                0, total_courts, 'running',
                'Starting court data collection',
                current_court='Initializing',
                stage='Starting scraper'
            )

            # Process each court type
            for ct in court_types:
                courts = get_courts_to_scrape(ct, court_ids)

                for court in courts:
                    try:
                        courts_processed += 1
                        logger.info(f"Processing {court['name']}")

                        # Update status
                        next_court = "Completion" if courts_processed == total_courts else f"Next court in queue"
                        update_scraper_status(
                            courts_processed, total_courts, 'running',
                            f'Processing {court["name"]}',
                            current_court=court['name'],
                            next_court=next_court,
                            stage='Fetching content'
                        )

                        if not court.get('url'):
                            logger.warning(f"No URL found for {court['name']}")
                            continue

                        text = get_court_data_from_url(court['url'])
                        if text:
                            update_scraper_status(
                                courts_processed, total_courts, 'running',
                                f'Extracting data from {court["name"]}',
                                current_court=court['name'],
                                next_court=next_court,
                                stage='Extracting data'
                            )

                            court_data = process_court_data(text, court)
                            if court_data:
                                court_data['id'] = court['id']
                                courts_data.append(court_data)
                                add_scraper_log('INFO', f'Successfully processed {court["name"]}', scraper_run_id)
                            else:
                                add_scraper_log('ERROR', f'Failed to extract data from {court["name"]}', scraper_run_id)

                        time.sleep(1)  # Rate limiting

                    except Exception as e:
                        error_message = f'Error processing {court["name"]}: {str(e)}'
                        logger.error(error_message)
                        add_scraper_log('ERROR', error_message, scraper_run_id)

            # Update final status
            completion_message = f'Completed processing {len(courts_data)} courts'
            update_scraper_status(
                courts_processed,
                total_courts,
                'completed',
                completion_message,
                current_court='Complete',
                stage='Finished'
            )

        return courts_data

    except Exception as e:
        logger.error(f"Error in scrape_courts: {str(e)}")
        return []

def update_database(courts_data: List[Dict]) -> None:
    """Update the database with new court data"""
    if not courts_data:
        logger.warning("No court data to update in database")
        return

    try:
        logger.info(f"Updating database with {len(courts_data)} courts")
        import psycopg2
        conn = psycopg2.connect(os.environ['DATABASE_URL'])
        cur = conn.cursor()

        for court in courts_data:
            cur.execute("""
                UPDATE courts SET
                    status = %s,
                    lat = %s,
                    lon = %s,
                    address = %s,
                    last_updated = CURRENT_TIMESTAMP
                WHERE id = %s
            """, (
                court['status'],
                float(court.get('lat', 0)),
                float(court.get('lon', 0)),
                court.get('address', 'Unknown'),
                court['id']
            ))

        conn.commit()
        logger.info("Database update completed successfully")
        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Error updating database: {str(e)}")
        raise

if __name__ == "__main__":
    logger.info("Starting court data collection...")
    courts_data = scrape_courts()

    if courts_data:
        logger.info(f"Updating database with {len(courts_data)} courts...")
        update_database(courts_data)
        logger.info("Database update complete.")
    else:
        logger.warning("No court data was collected")