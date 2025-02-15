import trafilatura
import pandas as pd
import json
from openai import OpenAI
import os
import time
from typing import List, Dict
from court_data import update_scraper_status, add_scraper_log, log_api_usage
from court_inventory import build_court_inventory

def get_court_data_from_url(url: str) -> str:
    """Fetch and extract text content from a URL"""
    downloaded = trafilatura.fetch_url(url)
    return trafilatura.extract(downloaded)

def process_court_data(text: str, court_info: Dict) -> Dict:
    """Process court data using OpenAI to extract structured information"""
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

    try:
        response = client.chat.completions.create(
            model="gpt-4o",  # Using the latest model
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": text}
            ],
            response_format={"type": "json_object"}
        )

        # Calculate tokens used
        tokens_used = len(text.split()) + len(system_prompt.split()) + len(response.choices[0].message.content.split())

        # Extract JSON from the response
        content = response.choices[0].message.content
        result = json.loads(content)

        # Log successful API usage
        log_api_usage(
            endpoint="chat.completions",
            tokens_used=tokens_used,
            model="gpt-4o",
            success=True
        )

        return result

    except Exception as e:
        # Log failed API usage
        log_api_usage(
            endpoint="chat.completions",
            tokens_used=0,
            model="gpt-4o",
            success=False,
            error_message=str(e)
        )
        print(f"Error processing court data: {e}")
        return None

def scrape_courts() -> List[Dict]:
    """Scrape court data using the inventory"""
    # First, build or update the court inventory
    courts_inventory = build_court_inventory()

    total_courts = len(courts_inventory)
    courts_data = []

    scraper_run_id = update_scraper_status(
        0, total_courts, 'running',
        'Starting court data collection',
        current_court='Initializing Inventory',
        stage='Building court inventory'
    )

    for i, court in enumerate(courts_inventory, 1):
        try:
            # Update status with current and next court
            next_court = courts_inventory[i]['name'] if i < len(courts_inventory) else "Completion"
            update_scraper_status(
                i, total_courts, 'running',
                f'Processing {court["name"]}',
                current_court=court['name'],
                next_court=next_court,
                stage='Fetching content'
            )

            add_scraper_log('INFO', f'Processing {court["name"]}: {court["url"]}', scraper_run_id)

            text = get_court_data_from_url(court['url'])
            if text:
                update_scraper_status(
                    i, total_courts, 'running',
                    f'Extracting data from {court["name"]}',
                    current_court=court['name'],
                    next_court=next_court,
                    stage='Extracting data'
                )

                court_data = process_court_data(text, court)
                if court_data:
                    courts_data.append(court_data)
                    add_scraper_log('INFO', f'Successfully processed {court["name"]}', scraper_run_id)
                else:
                    add_scraper_log('ERROR', f'Failed to extract data from {court["name"]}', scraper_run_id)

            time.sleep(1)  # Rate limiting

        except Exception as e:
            error_message = f'Error processing {court["name"]}: {str(e)}'
            add_scraper_log('ERROR', error_message, scraper_run_id)
            update_scraper_status(
                i, total_courts, 'error',
                error_message,
                current_court=court['name'],
                stage='Error'
            )
            print(error_message)

    completion_message = f'Completed processing {len(courts_data)} courts'
    add_scraper_log('INFO', completion_message, scraper_run_id)
    update_scraper_status(
        len(courts_data),
        total_courts,
        'completed',
        completion_message,
        current_court='Complete',
        stage='Finished'
    )

    return courts_data

def update_database(courts_data: List[Dict]) -> None:
    """Update the database with new court data"""
    import psycopg2
    from psycopg2.extras import execute_values

    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()

    # Insert new data
    insert_query = """
    INSERT INTO courts (
        name, type, status, lat, lon, address, image_url,
        jurisdiction_id, court_type_id
    ) VALUES %s
    ON CONFLICT (name) 
    DO UPDATE SET
        status = EXCLUDED.status,
        lat = EXCLUDED.lat,
        lon = EXCLUDED.lon,
        address = EXCLUDED.address,
        last_updated = CURRENT_TIMESTAMP
    """

    template_images = {
        'Supreme Court': 'https://images.unsplash.com/photo-1564596489416-23196d12d85c',
        'Courts of Appeals': 'https://images.unsplash.com/photo-1564595686486-c6e5cbdbe12c',
        'District Courts': 'https://images.unsplash.com/photo-1600786288398-e795cfac80aa',
        'Bankruptcy Courts': 'https://images.unsplash.com/photo-1521984692647-a41fed613ec7',
        'Other': 'https://images.unsplash.com/photo-1685747750264-a4e932005dde'
    }

    # Format data for insertion
    values = [(
        court['name'],
        court['type'],
        court['status'],
        float(court.get('lat', 0)),
        float(court.get('lon', 0)),
        court.get('address', 'Unknown'),
        template_images.get(court['type'], template_images['Other']),
        court.get('jurisdiction_id'),
        court.get('court_type_id')
    ) for court in courts_data]

    if values:
        execute_values(cur, insert_query, values)
        conn.commit()

    cur.close()
    conn.close()

if __name__ == "__main__":
    print("Building court inventory and gathering data...")
    courts_data = scrape_courts()

    print(f"Updating database with {len(courts_data)} courts...")
    update_database(courts_data)
    print("Database update complete.")