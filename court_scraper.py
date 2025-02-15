import trafilatura
import pandas as pd
import json
from openai import OpenAI
import os
import time
from typing import List, Dict
from court_data import update_scraper_status

def get_court_data_from_url(url: str) -> str:
    """Fetch and extract text content from a URL"""
    downloaded = trafilatura.fetch_url(url)
    return trafilatura.extract(downloaded)

def process_court_data(text: str) -> Dict:
    """Process court data using OpenAI to extract structured information"""
    client = OpenAI()

    prompt = """Extract the following information about the court from the text and return as JSON:
    {
        "name": "full court name",
        "type": "one of: Supreme Court, Appeals Court, District Court, Bankruptcy Court, or Other",
        "status": "one of: Open, Closed, Limited Operations",
        "address": "full address",
        "lat": latitude as float,
        "lon": longitude as float
    }
    If certain fields are missing, make educated guesses based on context.
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4o",  # the newest OpenAI model is "gpt-4o" which was released May 13, 2024
            messages=[
                {"role": "user", "content": f"{prompt}\n\nText: {text}"}
            ],
            response_format={"type": "json_object"}
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"Error processing court data: {e}")
        return None

def get_federal_courts() -> List[Dict]:
    """Get data for federal courts"""
    base_urls = [
        "https://www.supremecourt.gov",
        "https://www.uscourts.gov/about-federal-courts/court-website-links/court-website-links",
    ]

    courts_data = []
    total_courts = len(base_urls)  # Initial estimate
    update_scraper_status(0, total_courts, 'running', 'Starting court data collection')

    for i, url in enumerate(base_urls, 1):
        try:
            text = get_court_data_from_url(url)
            if text:
                court_data = process_court_data(text)
                if court_data:
                    courts_data.append(court_data)
                update_scraper_status(i, total_courts, 'running', f'Processed {url}')
                time.sleep(1)  # Rate limiting
        except Exception as e:
            update_scraper_status(i, total_courts, 'error', f'Error processing {url}: {e}')
            print(f"Error processing URL {url}: {e}")

    update_scraper_status(
        len(courts_data), 
        total_courts, 
        'completed', 
        f'Completed processing {len(courts_data)} courts'
    )
    return courts_data

def update_database(courts_data: List[Dict]) -> None:
    """Update the database with new court data"""
    import psycopg2
    from psycopg2.extras import execute_values

    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = conn.cursor()

    # Clear existing data
    cur.execute("TRUNCATE courts RESTART IDENTITY;")

    # Insert new data
    insert_query = """
    INSERT INTO courts (name, type, status, lat, lon, address, image_url)
    VALUES %s
    """

    # Add a default image URL for each court
    template_images = {
        'Supreme Court': 'https://images.unsplash.com/photo-1564596489416-23196d12d85c',
        'Appeals Court': 'https://images.unsplash.com/photo-1564595686486-c6e5cbdbe12c',
        'District Court': 'https://images.unsplash.com/photo-1600786288398-e795cfac80aa',
        'Bankruptcy Court': 'https://images.unsplash.com/photo-1521984692647-a41fed613ec7',
        'Other': 'https://images.unsplash.com/photo-1685747750264-a4e932005dde'
    }

    # Format data for insertion
    values = [(
        court['name'],
        court['type'],
        court['status'],
        court['lat'],
        court['lon'],
        court['address'],
        template_images.get(court['type'], template_images['Other'])
    ) for court in courts_data]

    execute_values(cur, insert_query, values)
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    # Collect and process court data
    print("Gathering court data...")
    courts_data = get_federal_courts()

    # Update database
    print(f"Updating database with {len(courts_data)} courts...")
    update_database(courts_data)
    print("Database update complete.")