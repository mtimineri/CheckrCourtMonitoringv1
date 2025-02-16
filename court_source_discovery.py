"""Module for discovering and validating court directory sources"""
import logging
import requests
from typing import List, Dict, Optional
from urllib.parse import urlparse
import os
from openai import OpenAI
from court_data import get_db_connection, return_db_connection
import json
import time
import re

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_court_url(url: str) -> bool:
    """Validate if a URL is accessible and likely a court website"""
    try:
        logger.info(f"Validating URL: {url}")
        # Add timeout and proper headers
        headers = {
            'User-Agent': 'Court Directory Validator/1.0',
            'Accept': 'text/html,application/xhtml+xml'
        }

        # Allow insecure SSL for certain government domains
        domain = urlparse(url).netloc.lower()
        verify_ssl = not any(d in domain for d in ['.phila.gov', '.lacourt.org', '.cookcountycourt.org'])

        response = requests.head(url, timeout=10, headers=headers, 
                               allow_redirects=True, verify=verify_ssl)

        if response.status_code == 404:
            logger.warning(f"URL not found: {url}")
            return False

        if response.status_code != 200:
            # Some government sites return 403 for HEAD requests
            if response.status_code == 403:
                # Try GET request instead
                response = requests.get(url, timeout=10, headers=headers,
                                     allow_redirects=True, verify=verify_ssl)
                if response.status_code != 200:
                    logger.warning(f"Invalid status code {response.status_code} for URL: {url}")
                    return False
            else:
                logger.warning(f"Invalid status code {response.status_code} for URL: {url}")
                return False

        logger.info(f"Checking domain: {domain}")

        court_indicators = [
            '.courts.', '.uscourts.', '.court.', 
            'supremecourt', 'judiciary', 'judicial',
            '.gov/courts', '/courts/', 'courtinfo',
            'lacourt', 'philacourts', 'cookcountycourt'
        ]

        # Additional validation for government domains
        is_gov_domain = (
            domain.endswith('.gov') or 
            domain.endswith('.us') or
            domain.endswith('court.org') or
            any(county in domain for county in ['lacourt', 'cookcountycourt'])
        )

        has_court_indicator = any(indicator in domain.lower() or indicator in url.lower() 
                                for indicator in court_indicators)

        if is_gov_domain:
            logger.info(f"Valid government domain found: {domain}")
        if has_court_indicator:
            logger.info(f"Valid court indicator found in URL: {url}")

        if not (is_gov_domain or has_court_indicator):
            logger.warning(f"URL {url} does not appear to be a valid court website")
            return False

        logger.info(f"Successfully validated URL: {url}")
        return True

    except requests.exceptions.SSLError as e:
        # Log but accept SSL errors for known government domains
        if any(d in domain for d in ['.phila.gov', '.lacourt.org', '.cookcountycourt.org']):
            logger.warning(f"Accepting URL despite SSL error for trusted domain: {url}")
            return True
        logger.error(f"SSL Error validating URL {url}: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error validating URL {url}: {str(e)}")
        return False

def extract_json_array(content: str) -> List[Dict]:
    """Extract JSON array from API response, handling various formats"""
    try:
        # First try direct JSON parsing
        data = json.loads(content)

        # Handle different response formats
        if isinstance(data, dict):
            # Check various common wrapper fields
            for field in ['response', 'result', 'results', 'sources', 'courts', 'data']:
                if field in data and isinstance(data[field], list):
                    return data[field]
            # If no wrapper field but single object, wrap in list
            if 'url' in data:
                return [data]
            # Try to find JSON array in message field
            if 'message' in data:
                try:
                    # Extract JSON array from message text
                    match = re.search(r'\[[\s\S]*\]', data['message'])
                    if match:
                        return json.loads(match.group(0))
                except:
                    pass
            return []
        elif isinstance(data, list):
            return data
        return []
    except json.JSONDecodeError:
        # Try to extract JSON array using regex if direct parsing fails
        try:
            # Look for JSON array pattern in the text
            match = re.search(r'\[[\s\S]*\]', content)
            if match:
                return json.loads(match.group(0))
            return []
        except Exception:
            logger.error(f"Failed to extract JSON from content: {content[:200]}...")
            return []

def discover_court_sources(jurisdiction_type: str) -> List[Dict]:
    """Use AI to discover court directory sources for a given jurisdiction type"""
    try:
        logger.info(f"Discovering sources for {jurisdiction_type} courts")
        client = OpenAI()

        # Sample list of known courts based on jurisdiction type
        example_courts = {
            'federal': [
                {'url': 'https://www.uscourts.gov', 'jurisdiction_name': 'United States', 'source_type': 'main'},
                {'url': 'https://www.supremecourt.gov', 'jurisdiction_name': 'United States', 'source_type': 'specialized'}
            ],
            'state': [
                {'url': 'https://www.courts.ca.gov', 'jurisdiction_name': 'California', 'source_type': 'main'},
                {'url': 'https://www.nycourts.gov', 'jurisdiction_name': 'New York', 'source_type': 'main'}
            ],
            'county': [
                {'url': 'https://www.cookcountycourt.org', 'jurisdiction_name': 'Cook County', 'source_type': 'main'},
                {'url': 'https://www.lacourt.org', 'jurisdiction_name': 'Los Angeles County', 'source_type': 'main'}
            ]
        }

        example_list = example_courts.get(jurisdiction_type, example_courts['state'])
        examples_str = json.dumps(example_list, indent=2)

        system_prompt = f"""You are a court system expert. Find official court directory websites for {jurisdiction_type} courts in the United States.

Return ONLY a JSON array following this exact format, with no additional text or explanation:
{examples_str}

Requirements for each entry:
- url: Must be a complete URL (including https://) for an official court website
- jurisdiction_name: Full name of the jurisdiction (e.g., "California" for state, "Cook County" for county)
- source_type: One of: "main" (primary court website), "specialized" (specific court type), "regional" (geographic division)

Focus ONLY on:
1. Official .gov or .us domains
2. State/federal judiciary websites
3. Official court directories
4. Administrative office websites

Exclude:
1. Third-party directories
2. Law firm websites
3. Bar association sites
4. Generic government portals"""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Return a JSON array of verified official court directory sources for {jurisdiction_type} courts"}
            ],
            response_format={"type": "json_object"}
        )

        content = response.choices[0].message.content.strip()
        logger.info(f"Received API response: {content}")

        sources = extract_json_array(content)
        logger.info(f"Found {len(sources)} potential sources for {jurisdiction_type} courts")

        # Validate discovered URLs
        validated_sources = []
        for source in sources:
            if not isinstance(source, dict):
                logger.warning(f"Invalid source format: {source}")
                continue

            url = source.get('url', '')
            if not url:
                logger.warning("Source missing URL field")
                continue

            if not url.startswith('http'):
                url = f"https://{url}"
                source['url'] = url
                logger.info(f"Added https:// to URL: {url}")

            if validate_court_url(url):
                validated_sources.append(source)
                logger.info(f"Added valid source: {url}")
            time.sleep(1)  # Rate limiting

        logger.info(f"Validated {len(validated_sources)} out of {len(sources)} sources")
        return validated_sources

    except Exception as e:
        logger.error(f"Error discovering court sources: {str(e)}")
        return []

def update_court_sources() -> Dict:
    """Update the database with newly discovered court sources"""
    conn = None
    try:
        logger.info("Starting court sources update process")
        conn = get_db_connection()
        if conn is None:
            return {'status': 'error', 'message': 'Database connection failed'}

        cur = conn.cursor()

        # Track statistics
        new_sources = 0
        updated_sources = 0

        # Process each jurisdiction type
        jurisdiction_types = ['federal', 'state', 'county']

        for jtype in jurisdiction_types:
            logger.info(f"Discovering sources for {jtype} courts")
            sources = discover_court_sources(jtype)

            for source in sources:
                try:
                    # First, check if jurisdiction exists
                    cur.execute("""
                        INSERT INTO jurisdictions (name, type)
                        VALUES (%s, %s)
                        ON CONFLICT (name, type) DO NOTHING
                        RETURNING id;
                    """, (source['jurisdiction_name'], jtype))

                    jurisdiction_id = cur.fetchone()
                    if jurisdiction_id is None:
                        # If no id was returned, get the existing jurisdiction's id
                        cur.execute("""
                            SELECT id FROM jurisdictions 
                            WHERE name = %s AND type = %s
                        """, (source['jurisdiction_name'], jtype))
                        jurisdiction_id = cur.fetchone()

                    if jurisdiction_id:
                        jurisdiction_id = jurisdiction_id[0]
                        # Then, add/update the court source
                        cur.execute("""
                            INSERT INTO court_sources 
                            (jurisdiction_id, source_url, source_type, is_active, last_checked)
                            VALUES (%s, %s, %s, true, CURRENT_TIMESTAMP)
                            ON CONFLICT (jurisdiction_id, source_url) 
                            DO UPDATE SET 
                                source_type = EXCLUDED.source_type,
                                is_active = true,
                                last_checked = CURRENT_TIMESTAMP,
                                last_updated = CASE 
                                    WHEN court_sources.source_type != EXCLUDED.source_type 
                                    THEN CURRENT_TIMESTAMP 
                                    ELSE court_sources.last_updated 
                                END
                            RETURNING (xmax = 0) as is_insert;
                        """, (jurisdiction_id, source['url'], source['source_type']))

                        is_insert = cur.fetchone()
                        if is_insert and is_insert[0]:
                            new_sources += 1
                            logger.info(f"Added new source: {source['url']}")
                        else:
                            updated_sources += 1
                            logger.info(f"Updated existing source: {source['url']}")
                    else:
                        logger.error(f"Failed to get jurisdiction ID for {source['jurisdiction_name']}")

                except Exception as e:
                    logger.error(f"Error adding source {source['url']}: {str(e)}")
                    continue

            conn.commit()
            time.sleep(2)  # Rate limiting between jurisdiction types

        cur.close()
        logger.info(f"Completed update: {new_sources} new sources, {updated_sources} updated")
        return {
            'status': 'completed',
            'new_sources': new_sources,
            'updated_sources': updated_sources
        }

    except Exception as e:
        logger.error(f"Error updating court sources: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }
    finally:
        if conn:
            return_db_connection(conn)

if __name__ == "__main__":
    result = update_court_sources()
    print(f"Update completed with result: {result}")