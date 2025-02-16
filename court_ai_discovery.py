import json
import os
from typing import Dict, List, Optional
import logging
from openai import OpenAI
import trafilatura
from urllib.parse import urljoin
import re
import urllib3
import psycopg2
from court_data import get_db_connection

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize OpenAI client
# the newest OpenAI model is "gpt-4o" which was released May 13, 2024.
# do not change this unless explicitly requested by the user
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def search_court_directories() -> List[str]:
    """Use OpenAI to generate a list of potential court directory URLs"""
    try:
        system_prompt = """You are a court information specialist. Generate a list of valid, accessible court directory URLs for the United States. Return ONLY an array of direct URLs in JSON format, like this:
{
    "urls": [
        "https://www.uscourts.gov",
        "https://www.supremecourt.gov"
    ]
}

Focus on:
1. Main federal court websites
2. State supreme court websites
3. Major district court portals
4. Bankruptcy court directories

Rules:
1. Use only .gov or .us domains when possible
2. Ensure URLs are direct links (no search pages)
3. Include only base domains, no query parameters
4. No parenthetical text or spaces in URLs"""

        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": "Generate a list of valid US court directory URLs"}
                ],
                response_format={"type": "json_object"}
            )

            result = json.loads(response.choices[0].message.content)
            urls = result.get('urls', [])

            # Clean and validate URLs
            valid_urls = []
            for url in urls:
                if isinstance(url, str):  # Ensure URL is a string
                    cleaned_url = clean_and_validate_url(url)
                    if cleaned_url:
                        valid_urls.append(cleaned_url)
                        logger.info(f"Added valid URL: {cleaned_url}")
                else:
                    logger.warning(f"Skipping invalid URL format: {url}")
                    continue

            logger.info(f"Found {len(valid_urls)} valid court directory URLs")
            return valid_urls

        except Exception as e:
            logger.error(f"Error in OpenAI API call: {str(e)}")
            # Return a default list of well-known court URLs as fallback
            default_urls = [
                "https://www.uscourts.gov",
                "https://www.supremecourt.gov",
                "https://www.ca1.uscourts.gov",
                "https://www.ca2.uscourts.gov",
                "https://www.ca3.uscourts.gov"
            ]
            logger.info("Using default court URLs as fallback")
            return default_urls

    except Exception as e:
        logger.error(f"Error searching court directories: {str(e)}")
        return []

def store_discovered_court(court_data: Dict) -> bool:
    """Store discovered court in the database"""
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("Failed to get database connection")
            return False

        cur = conn.cursor()
        try:
            # Check if court already exists
            cur.execute("""
                SELECT id FROM courts 
                WHERE name = %s AND jurisdiction_id = (
                    SELECT id FROM jurisdictions WHERE name = %s
                )
            """, (court_data['name'], court_data.get('jurisdiction', 'Federal')))

            existing_court = cur.fetchone()

            if existing_court:
                # Update existing court
                cur.execute("""
                    UPDATE courts 
                    SET type = %s,
                        status = %s,
                        address = %s,
                        last_updated = CURRENT_TIMESTAMP,
                        contact_info = %s
                    WHERE id = %s
                """, (
                    court_data['type'],
                    court_data['status'],
                    court_data.get('address'),
                    json.dumps(court_data.get('contact_info', {})),
                    existing_court[0]
                ))
                logger.info(f"Updated existing court: {court_data['name']}")
            else:
                # Get or create jurisdiction
                jurisdiction_type = 'federal' if 'Federal' in court_data['type'] else 'state'
                cur.execute("""
                    INSERT INTO jurisdictions (name, type)
                    VALUES (%s, %s)
                    ON CONFLICT (name) DO UPDATE SET type = EXCLUDED.type
                    RETURNING id
                """, (court_data.get('jurisdiction', 'Federal'), jurisdiction_type))

                jurisdiction_id = cur.fetchone()[0]

                # Insert new court
                cur.execute("""
                    INSERT INTO courts (
                        name, type, status, jurisdiction_id, 
                        address, contact_info, last_updated
                    ) VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                """, (
                    court_data['name'],
                    court_data['type'],
                    court_data['status'],
                    jurisdiction_id,
                    court_data.get('address'),
                    json.dumps(court_data.get('contact_info', {}))
                ))
                logger.info(f"Inserted new court: {court_data['name']}")

            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logger.error(f"Database error storing court {court_data['name']}: {str(e)}")
            return False
        finally:
            cur.close()
            conn.close()

    except Exception as e:
        logger.error(f"Error storing court data: {str(e)}")
        return False

def validate_url(url: str) -> bool:
    """Validate URL format and accessibility"""
    try:
        # Clean up the URL - remove spaces and parenthetical text
        cleaned_url = url.split('(')[0].strip()
        if not cleaned_url.startswith(('http://', 'https://')):
            cleaned_url = 'https://' + cleaned_url

        # Basic URL format validation
        if not re.match(r'^https?://[\w\-.]+(:\d+)?(/[\w\-./?%&=]*)?$', cleaned_url):
            logger.warning(f"Invalid URL format: {url}")
            return False

        # Test URL accessibility with SSL verification disabled
        downloaded = trafilatura.fetch_url(cleaned_url, ssl_verify=False)
        if not downloaded:
            logger.warning(f"Unable to access URL: {cleaned_url}")
            return False

        return True
    except Exception as e:
        logger.error(f"Error validating URL {url}: {str(e)}")
        return False

def clean_and_validate_url(url: str) -> Optional[str]:
    """Clean and validate a URL, returning None if invalid"""
    try:
        # Clean up the URL
        cleaned_url = url.split('(')[0].strip()
        if not cleaned_url.startswith(('http://', 'https://')):
            cleaned_url = 'https://' + cleaned_url

        # Validate format
        if not re.match(r'^https?://[\w\-.]+(:\d+)?(/[\w\-./?%&=]*)?$', cleaned_url):
            return None

        return cleaned_url
    except Exception:
        return None

def process_court_page(url: str) -> List[Dict]:
    """Process a court webpage and extract verified court information"""
    try:
        logger.info(f"Starting to process URL: {url}")

        # Clean up the URL - remove spaces and parenthetical text
        cleaned_url = url.split('(')[0].strip()
        if not cleaned_url.startswith(('http://', 'https://')):
            cleaned_url = 'https://' + cleaned_url

        if not validate_url(cleaned_url):
            logger.warning(f"Skipping invalid or inaccessible URL: {url}")
            return []

        logger.info(f"Fetching content from {cleaned_url}")
        downloaded = trafilatura.fetch_url(cleaned_url)
        if not downloaded:
            logger.warning(f"Failed to download content from {cleaned_url}")
            return []

        content = trafilatura.extract(downloaded, include_links=True, include_tables=True)
        if not content:
            logger.warning(f"No content extracted from {cleaned_url}")
            return []

        logger.info(f"Successfully extracted content from {cleaned_url}")
        courts = discover_courts_from_content(content, cleaned_url)

        # Verify each court before returning
        verified_courts = []
        for court in courts:
            verified_court = verify_court_info(court)
            if verified_court.get('verified', False):
                verified_courts.append(verified_court)
                logger.info(f"Verified court: {verified_court.get('name', 'Unknown')}")
            else:
                logger.warning(f"Court verification failed: {court.get('name', 'Unknown')}")

        logger.info(f"Found {len(verified_courts)} verified courts from {cleaned_url}")
        return verified_courts

    except Exception as e:
        logger.error(f"Error processing court page {url}: {str(e)}")
        return []

def search_court_directories() -> List[str]:
    """Use OpenAI to generate a list of potential court directory URLs"""
    try:
        system_prompt = """You are a court information specialist. Generate a list of valid, accessible court directory URLs for the United States. Return ONLY an array of direct URLs in JSON format, like this:
{
    "urls": [
        "https://www.uscourts.gov",
        "https://www.supremecourt.gov"
    ]
}

Focus on:
1. Main federal court websites
2. State supreme court websites
3. Major district court portals
4. Bankruptcy court directories

Rules:
1. Use only .gov or .us domains when possible
2. Ensure URLs are direct links (no search pages)
3. Include only base domains, no query parameters
4. No parenthetical text or spaces in URLs"""

        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": "Generate a list of valid US court directory URLs"}
                ],
                response_format={"type": "json_object"}
            )

            result = json.loads(response.choices[0].message.content)
            urls = result.get('urls', [])

            # Clean and validate URLs
            valid_urls = []
            for url in urls:
                if isinstance(url, str):  # Ensure URL is a string
                    cleaned_url = clean_and_validate_url(url)
                    if cleaned_url:
                        valid_urls.append(cleaned_url)
                        logger.info(f"Added valid URL: {cleaned_url}")
                else:
                    logger.warning(f"Skipping invalid URL format: {url}")
                    continue

            logger.info(f"Found {len(valid_urls)} valid court directory URLs")
            return valid_urls

        except Exception as e:
            logger.error(f"Error in OpenAI API call: {str(e)}")
            # Return a default list of well-known court URLs as fallback
            default_urls = [
                "https://www.uscourts.gov",
                "https://www.supremecourt.gov",
                "https://www.ca1.uscourts.gov",
                "https://www.ca2.uscourts.gov",
                "https://www.ca3.uscourts.gov"
            ]
            logger.info("Using default court URLs as fallback")
            return default_urls

    except Exception as e:
        logger.error(f"Error searching court directories: {str(e)}")
        return []

def verify_court_info(court_data: Dict) -> Dict:
    """
    Use OpenAI to verify and enrich court information
    """
    try:
        system_prompt = """You are a court information verification expert. Analyze the provided court information and:
1. Verify if this appears to be a legitimate court
2. Classify the court type into one of these categories:
   - Supreme Court
   - Courts of Appeals
   - District Courts
   - Bankruptcy Courts
   - Specialized Federal Courts
   - State Supreme Courts
   - State Appellate Courts
   - State Trial Courts
   - State Specialized Courts
   - County Superior Courts
   - County Circuit Courts
   - County District Courts
   - County Family Courts
   - County Probate Courts
   - County Criminal Courts
   - County Civil Courts
   - County Juvenile Courts
   - County Small Claims Courts
   - Municipal Courts
   - Tribal Courts
   - Administrative Courts
3. Provide a confidence score (0-1)
4. Extract or validate the court's address
5. Determine operating status: Open, Closed, or Limited Operations
6. Extract contact information and operating hours if available

Respond with a JSON object containing:
{
    "verified": boolean,
    "confidence": float,
    "court_type": string,
    "status": string,
    "address": string or null,
    "contact_info": {
        "phone": string or null,
        "email": string or null,
        "hours": string or null
    },
    "additional_info": string or null
}"""

        user_prompt = f"Verify this court information:\n{json.dumps(court_data, indent=2)}"

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)

        # Update court data with verified information
        court_data.update({
            'verified': result['verified'],
            'confidence': result['confidence'],
            'type': result['court_type'],
            'status': result['status'],
            'address': result['address'],
            'contact_info': result.get('contact_info', {}),
            'additional_info': result['additional_info']
        })

        return court_data

    except Exception as e:
        logger.error(f"Error verifying court info: {str(e)}")
        return court_data

def discover_courts_from_content(content: str, base_url: str) -> List[Dict]:
    """Use OpenAI to discover courts from webpage content"""
    try:
        if not content.strip():
            logger.warning("Empty content provided for court discovery")
            return []

        system_prompt = """As a court information extraction expert, analyze the provided webpage content and identify all courts mentioned. Extract:

1. Court names and types (be specific about jurisdiction level)
2. Jurisdictional information (federal, state, county, municipal, or tribal)
3. Physical locations and addresses
4. Contact information (phone, email)
5. Operating status and hours
6. URLs for each court
7. Any special divisions or departments
8. Additional services provided

For each court found, create a JSON object with:
{
    "name": string,
    "type": string,
    "jurisdiction": string,
    "address": string or null,
    "url": string or null,
    "status": string,
    "contact_info": {
        "phone": string or null,
        "email": string or null,
        "hours": string or null
    },
    "divisions": [string],
    "services": [string]
}

Return a JSON object with an array of courts:
{
    "courts": []
}"""

        user_prompt = f"Extract court information from this webpage content:\n{content}"

        try:
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                response_format={"type": "json_object"}
            )

            result = json.loads(response.choices[0].message.content)
            courts = result.get('courts', [])

            # Verify each discovered court
            verified_courts = []
            for court in courts:
                if not court.get('url') and 'name' in court:
                    # Try to find URL in original content
                    court_name_pattern = re.escape(court['name'])
                    url_match = re.search(f'href=[\'"]([^\'"]*){court_name_pattern}[\'"]', content)
                    if url_match:
                        matched_url = url_match.group(1)
                        # Ensure URL is absolute
                        court['url'] = urljoin(base_url, matched_url)

                verified_court = verify_court_info(court)
                if verified_court.get('verified', False) and verified_court.get('confidence', 0) > 0.7:
                    verified_courts.append(verified_court)

            logger.info(f"Discovered and verified {len(verified_courts)} courts from content at {base_url}")
            return verified_courts

        except Exception as e:
            logger.error(f"Error in OpenAI API call: {str(e)}")
            return []

    except Exception as e:
        logger.error(f"Error discovering courts: {str(e)}")
        return []

def process_court_page(url: str) -> List[Dict]:
    """Process a court webpage and extract verified court information"""
    try:
        logger.info(f"Starting to process URL: {url}")

        # Clean up the URL - remove spaces and parenthetical text
        cleaned_url = url.split('(')[0].strip()
        if not cleaned_url.startswith(('http://', 'https://')):
            cleaned_url = 'https://' + cleaned_url

        if not validate_url(cleaned_url):
            logger.warning(f"Skipping invalid or inaccessible URL: {url}")
            return []

        logger.info(f"Fetching content from {cleaned_url}")
        downloaded = trafilatura.fetch_url(cleaned_url)
        if not downloaded:
            logger.warning(f"Failed to download content from {cleaned_url}")
            return []

        content = trafilatura.extract(downloaded, include_links=True, include_tables=True)
        if not content:
            logger.warning(f"No content extracted from {cleaned_url}")
            return []

        logger.info(f"Successfully extracted content from {cleaned_url}")
        courts = discover_courts_from_content(content, cleaned_url)

        # Verify each court before returning
        verified_courts = []
        for court in courts:
            verified_court = verify_court_info(court)
            if verified_court.get('verified', False):
                verified_courts.append(verified_court)
                logger.info(f"Verified court: {verified_court.get('name', 'Unknown')}")
            else:
                logger.warning(f"Court verification failed: {court.get('name', 'Unknown')}")

        logger.info(f"Found {len(verified_courts)} verified courts from {cleaned_url}")
        return verified_courts

    except Exception as e:
        logger.error(f"Error processing court page {url}: {str(e)}")
        return []