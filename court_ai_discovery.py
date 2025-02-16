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
import time
from court_data import get_db_connection, return_db_connection

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize OpenAI client
# Note: We're using gpt-4o-mini as it's more efficient for this task
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def initialize_ai_discovery():
    """Initialize the AI discovery module"""
    logger.info("Initializing AI discovery module...")
    try:
        # Test OpenAI API key
        if not os.environ.get("OPENAI_API_KEY"):
            logger.error("OpenAI API key not found")
            return False

        # Test OpenAI API with a simple prompt
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",  
                messages=[
                    {"role": "user", "content": "Test connection"}
                ]
            )
            logger.info("Successfully tested OpenAI API connection")
            return True
        except Exception as e:
            logger.error(f"Failed to test OpenAI API: {str(e)}")
            return False

    except Exception as e:
        logger.error(f"Error initializing AI discovery: {str(e)}")
        return False

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
                model="gpt-4o-mini",  
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
                if isinstance(url, str):  
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
    """Store discovered court in the database with improved error handling and connection management"""
    conn = None
    try:
        if not court_data.get('name') or not court_data.get('jurisdiction'):
            logger.error("Cannot store court without name and jurisdiction")
            return False

        max_retries = 3
        for attempt in range(max_retries):
            try:
                conn = get_db_connection()
                if conn is None:
                    logger.error(f"Failed to get database connection (attempt {attempt + 1}/{max_retries})")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    return False
                break
            except Exception as e:
                logger.error(f"Database connection error (attempt {attempt + 1}/{max_retries}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return False

        cur = conn.cursor()
        try:
            # Get or create jurisdiction first
            jurisdiction_name = court_data.get('jurisdiction', 'Federal')
            jurisdiction_type = court_data.get('jurisdiction_type', 'federal')

            logger.info(f"Creating/updating jurisdiction: {jurisdiction_name} ({jurisdiction_type})")
            cur.execute("""
                INSERT INTO jurisdictions (name, type)
                VALUES (%s, %s)
                ON CONFLICT (name) DO UPDATE SET type = EXCLUDED.type
                RETURNING id
            """, (jurisdiction_name, jurisdiction_type))

            jurisdiction_id = cur.fetchone()
            if not jurisdiction_id:
                logger.error(f"Failed to get jurisdiction ID for {jurisdiction_name}")
                conn.rollback()
                return False

            jurisdiction_id = jurisdiction_id[0]

            # Insert or update court with jurisdiction
            # Use name and jurisdiction_id as composite unique identifier
            cur.execute("""
                INSERT INTO courts (
                    name, type, status, jurisdiction_id, 
                    address, contact_info, last_updated
                ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, CURRENT_TIMESTAMP)
                ON CONFLICT (name, jurisdiction_id) DO UPDATE SET
                    type = EXCLUDED.type,
                    status = EXCLUDED.status,
                    address = EXCLUDED.address,
                    contact_info = EXCLUDED.contact_info,
                    last_updated = CURRENT_TIMESTAMP
                RETURNING id
            """, (
                court_data['name'],
                court_data.get('type'),
                court_data.get('status'),
                jurisdiction_id,
                court_data.get('address'),
                json.dumps(court_data.get('contact_info', {}))
            ))

            court_id = cur.fetchone()
            if not court_id:
                logger.error(f"Failed to insert/update court {court_data['name']}")
                conn.rollback()
                return False

            conn.commit()
            logger.info(f"Successfully stored/updated court: {court_data['name']} in jurisdiction: {jurisdiction_name}")
            return True

        except psycopg2.Error as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error storing court {court_data.get('name', 'Unknown')}: {str(e)}", exc_info=True)
            return False
        finally:
            if cur:
                cur.close()

    except Exception as e:
        logger.error(f"Error storing court data: {str(e)}", exc_info=True)
        return False
    finally:
        if conn:
            try:
                conn.close()
                logger.debug("Database connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing database connection: {str(e)}")

    return False

def validate_url(url: str) -> tuple[bool, str]:
    """Validate URL format and accessibility, return tuple of (is_valid, reason)"""
    try:
        # Clean up the URL
        cleaned_url = url.split('(')[0].strip()
        if not cleaned_url.startswith(('http://', 'https://')):
            cleaned_url = 'https://' + cleaned_url

        # Basic URL format validation
        if not re.match(r'^https?://[\w\-.]+(:\d+)?(/[\w\-./?%&=]*)?$', cleaned_url):
            logger.warning(f"Invalid URL format: {url}")
            return False, "invalid_format"

        # Test URL accessibility with retry logic and SSL verification disabled for testing
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # First try with SSL verification
                downloaded = trafilatura.fetch_url(cleaned_url)
                if downloaded:
                    return True, "success"

                # If failed, try without SSL verification
                downloaded = trafilatura.fetch_url(cleaned_url, verify=False)
                if downloaded:
                    logger.warning(f"URL {cleaned_url} accessible only with SSL verification disabled")
                    return False, "ssl_verification_failed"

                logger.warning(f"Attempt {attempt + 1}: Unable to access URL: {cleaned_url}")
                time.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                error_message = str(e)
                logger.warning(f"Attempt {attempt + 1} failed for {cleaned_url}: {error_message}")

                # Check for different types of errors
                if "No address associated with hostname" in error_message:
                    return False, "dns_error"
                elif "too many redirects" in error_message:
                    return False, "redirect_loop"
                elif "CERTIFICATE_VERIFY_FAILED" in error_message:
                    return False, "ssl_cert_invalid"
                elif "SSLError" in error_message:
                    return False, "ssl_error"

                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                continue

        return False, "connection_failed"
    except Exception as e:
        logger.error(f"Error validating URL {url}: {str(e)}")
        return False, f"error: {str(e)}"

def store_invalid_url(url: str, reason: str):
    """Store invalid URL in the database to avoid future attempts"""
    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to get database connection")
        return False

    try:
        cur = conn.cursor()
        # Create table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS invalid_urls (
                url TEXT PRIMARY KEY,
                reason TEXT NOT NULL,
                first_failure TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_checked TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Insert or update invalid URL
        cur.execute("""
            INSERT INTO invalid_urls (url, reason)
            VALUES (%s, %s)
            ON CONFLICT (url) 
            DO UPDATE SET 
                last_checked = CURRENT_TIMESTAMP,
                reason = EXCLUDED.reason
        """, (url, reason))

        conn.commit()
        logger.info(f"Stored invalid URL {url} with reason: {reason}")
        return True
    except Exception as e:
        logger.error(f"Error storing invalid URL {url}: {str(e)}")
        if conn:
            conn.rollback()
        return False
    finally:
        if cur:
            cur.close()
        return_db_connection(conn)

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
    """Process a court webpage and extract verified court information with improved error handling"""
    try:
        logger.info(f"Starting to process URL: {url}")

        # Clean up the URL
        cleaned_url = clean_and_validate_url(url)
        if not cleaned_url:
            logger.warning(f"Invalid URL format: {url}")
            return []

        # Use retry logic with exponential backoff
        max_retries = 3
        downloaded = None

        for attempt in range(max_retries):
            try:
                downloaded = trafilatura.fetch_url(cleaned_url)
                if downloaded:
                    break

                backoff_time = min(2 ** attempt, 10)  # Exponential backoff, max 10 seconds
                logger.warning(f"Attempt {attempt + 1}: Unable to download content from {cleaned_url}. "
                             f"Retrying in {backoff_time} seconds...")
                time.sleep(backoff_time)

            except Exception as e:
                backoff_time = min(2 ** attempt, 10)
                logger.warning(f"Download attempt {attempt + 1} failed for {cleaned_url}: {str(e)}. "
                             f"Retrying in {backoff_time} seconds...")
                if attempt < max_retries - 1:
                    time.sleep(backoff_time)
                continue

        if not downloaded:
            logger.warning(f"Failed to download content from {cleaned_url} after {max_retries} attempts")
            return []

        content = trafilatura.extract(downloaded, include_links=True, include_tables=True)
        if not content:
            logger.warning(f"No content extracted from {cleaned_url}")
            return []

        logger.info(f"Successfully extracted content from {cleaned_url}, content length: {len(content)}")

        # Process content in chunks if too large
        max_chunk_size = 4000
        content_chunks = [content[i:i + max_chunk_size] 
                        for i in range(0, len(content), max_chunk_size)]

        all_courts = []
        for chunk in content_chunks:
            try:
                courts = discover_courts_from_content(chunk, cleaned_url)
                all_courts.extend(courts)
            except Exception as e:
                logger.error(f"Error processing content chunk from {cleaned_url}: {str(e)}")
                continue

        logger.info(f"Discovered {len(all_courts)} potential courts from content")

        # Verify each court before returning
        verified_courts = []
        for court in all_courts:
            try:
                verified_court = verify_court_info(court)
                if verified_court.get('verified', False):
                    verified_courts.append(verified_court)
                    logger.info(f"Verified court: {verified_court.get('name', 'Unknown')}")
                else:
                    logger.warning(f"Court verification failed for {court.get('name', 'Unknown')}: "
                                 f"{verified_court.get('message', 'Unknown reason')}")
            except Exception as e:
                logger.error(f"Error verifying court {court.get('name', 'Unknown')}: {str(e)}")
                continue

        logger.info(f"Found {len(verified_courts)} verified courts from {cleaned_url}")
        return verified_courts

    except Exception as e:
        logger.error(f"Error processing court page {url}: {str(e)}", exc_info=True)
        return []

def verify_court_info(court_data: Dict) -> Dict:
    """Use OpenAI to verify and enrich court information"""
    try:
        logger.info(f"Starting court verification for: {court_data.get('name', 'Unknown Court')}")

        system_prompt = """You are a court information verification expert. Analyze the provided court information and:
1. Verify if this appears to be a legitimate court
2. Classify the court type accurately
3. Provide a confidence score (0-1)
4. Validate or enhance the address
5. Determine operating status
6. Validate contact information
7. Ensure the jurisdiction information is complete and accurate

Respond with a JSON object containing:
{
    "verified": boolean,
    "confidence": float,
    "type": string,
    "status": string,
    "address": string or null,
    "jurisdiction": string,
    "jurisdiction_type": string,
    "contact_info": {
        "phone": string or null,
        "email": string or null,
        "hours": string or null
    },
    "message": string
}"""

        try:
            logger.info("Making OpenAI API call for court verification")
            response = client.chat.completions.create(
                model="gpt-4o-mini",  
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Verify this court information:\n{json.dumps(court_data, indent=2)}"}
                ],
                response_format={"type": "json_object"}
            )
            logger.info("Successfully received OpenAI API response for verification")

            result = json.loads(response.choices[0].message.content)

            # Update court data with verified information
            court_data.update({
                'verified': result['verified'],
                'confidence': result['confidence'],
                'type': result['type'],
                'status': result['status'],
                'address': result['address'],
                'jurisdiction': result['jurisdiction'],
                'jurisdiction_type': result['jurisdiction_type'],
                'contact_info': result.get('contact_info', {}),
                'message': result.get('message')
            })

            logger.info(f"Court verification completed with confidence: {result['confidence']}")
            return court_data

        except Exception as e:
            logger.error(f"Error in OpenAI API call during verification: {str(e)}")
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

        logger.info(f"Starting AI discovery for content from {base_url}")

        system_prompt = """As a court information extraction expert, analyze the provided webpage content and identify all courts mentioned. Include jurisdiction details and unique identifiers. Extract:

1. Court names (be specific and include state/district/circuit if mentioned)
2. Jurisdictional information (federal, state, county, municipal, or tribal)
3. Physical locations and addresses
4. Contact information
5. Operating status and hours
6. URLs for each court
7. Special divisions or departments
8. Additional services

For each court found, create a JSON object with:
{
    "name": string (full official name including jurisdiction),
    "type": string,
    "jurisdiction": string (specific jurisdiction name),
    "jurisdiction_type": string (federal/state/county/municipal/tribal),
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

        try:
            logger.info("Making OpenAI API call for court discovery")
            response = client.chat.completions.create(
                model="gpt-4o-mini",  
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Extract court information from this webpage content:\n{content[:8000]}"}
                ],
                response_format={"type": "json_object"}
            )
            logger.info("Successfully received OpenAI API response")

            result = json.loads(response.choices[0].message.content)
            courts = result.get('courts', [])

            # Add URLs to courts if found in content
            for court in courts:
                if not court.get('url') and 'name' in court:
                    court_name_pattern = re.escape(court['name'])
                    url_match = re.search(f'href=[\'"]([^\'"]*){court_name_pattern}[\'"]', content)
                    if url_match:
                        matched_url = url_match.group(1)
                        court['url'] = urljoin(base_url, matched_url)

            logger.info(f"Discovered {len(courts)} courts from content at {base_url}")
            return courts

        except Exception as e:
            logger.error(f"Error in OpenAI API call: {str(e)}")
            return []

    except Exception as e:
        logger.error(f"Error discovering courts: {str(e)}")
        return []

def test_discovery_process():
    """Test the entire discovery process end-to-end with improved error handling and timeouts"""
    try:
        logger.info("Starting discovery process test")

        # Initialize AI
        if not initialize_ai_discovery():
            logger.error("Failed to initialize AI discovery")
            return False

        # Get URLs with timeout
        urls = search_court_directories()
        logger.info(f"Found {len(urls)} court directory URLs")

        total_courts_found = 0
        total_courts_stored = 0
        failed_courts = []
        skipped_urls = []
        invalid_urls = []

        # Process all URLs with per-URL timeout
        for url in urls:
            logger.info(f"\nProcessing URL: {url}")
            try:
                # Set a maximum time limit for each URL
                start_time = time.time()
                max_url_time = 180  # 3 minutes max per URL

                # Validate URL first
                is_valid, reason = validate_url(url)
                if not is_valid:
                    logger.warning(f"Invalid URL {url}: {reason}")
                    invalid_urls.append((url, reason))
                    store_invalid_url(url, reason)
                    continue

                courts = process_court_page(url)

                if time.time() - start_time > max_url_time:
                    logger.warning(f"URL processing time exceeded limit for {url}")
                    skipped_urls.append(url)
                    continue

                if courts:
                    logger.info(f"Found {len(courts)} courts from {url}")
                    total_courts_found += len(courts)

                    for court in courts:
                        try:
                            if store_discovered_court(court):
                                total_courts_stored += 1
                            else:
                                failed_courts.append(court.get('name', 'Unknown'))
                                logger.warning(f"Failed to store court: {court.get('name', 'Unknown')}")
                        except Exception as e:
                            failed_courts.append(court.get('name', 'Unknown'))
                            logger.error(f"Error storing court {court.get('name', 'Unknown')}: {str(e)}")
                            continue

                # Add a small delay between requests
                time.sleep(2)

            except Exception as e:
                logger.error(f"Error processing URL {url}: {str(e)}", exc_info=True)
                skipped_urls.append(url)
                continue

        # Log detailed results
        logger.info(f"\nTest Results:")
        logger.info(f"Total courts found: {total_courts_found}")
        logger.info(f"Total courts stored: {total_courts_stored}")
        if failed_courts:
            logger.warning(f"Failed to store {len(failed_courts)} courts: {', '.join(failed_courts)}")
        if skipped_urls:
            logger.warning(f"Skipped {len(skipped_urls)} URLs due to errors or timeouts: {', '.join(skipped_urls)}")
        if invalid_urls:
            logger.warning(f"Invalid URLs found: {len(invalid_urls)}")
            for url, reason in invalid_urls:
                logger.warning(f"  - {url}: {reason}")

        return True

    except Exception as e:
        logger.error(f"Error in discovery process test: {str(e)}", exc_info=True)
        return False