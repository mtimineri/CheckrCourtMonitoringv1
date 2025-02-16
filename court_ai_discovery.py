import json
import os
from typing import Dict, List, Optional
import logging
from openai import OpenAI
import trafilatura
from urllib.parse import urljoin
import re

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize OpenAI client
# the newest OpenAI model is "gpt-4o" which was released May 13, 2024.
# do not change this unless explicitly requested by the user
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

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
        downloaded = trafilatura.fetch_url(cleaned_url, verify=False)
        if not downloaded:
            logger.warning(f"Unable to access URL: {cleaned_url}")
            return False

        return True
    except Exception as e:
        logger.error(f"Error validating URL {url}: {str(e)}")
        return False

def search_court_directories() -> List[str]:
    """
    Use OpenAI to generate a list of potential court directory URLs
    """
    try:
        system_prompt = """As a court directory expert, generate a comprehensive list of official court directory URLs in the United States. Include:

1. Federal Courts:
   - Supreme Court
   - Courts of Appeals (all circuits)
   - District Courts
   - Bankruptcy Courts
   - Federal Judicial Center
   - Court of Federal Claims
   - Court of International Trade

2. State Courts for all 50 states:
   - State Supreme Courts
   - State Courts of Appeals
   - State Trial Courts
   - State Judicial Branch websites
   - State Administrative Courts
   - State Tax Courts
   - State Workers' Compensation Courts

3. County Courts (for major counties in each state):
   - Superior Courts
   - Circuit Courts
   - District Courts
   - Family Courts
   - Probate Courts
   - Criminal Courts
   - Civil Courts
   - Juvenile Courts
   - Small Claims Courts

4. Municipal Courts:
   - City Courts
   - Town Courts
   - Village Courts
   - Traffic Courts

5. Tribal Courts:
   - Tribal Supreme Courts
   - Tribal District Courts
   - Tribal Administrative Courts

Focus on official government domains (.gov, .us) and state judicial websites.
Return a JSON object with the following structure:
{
    "federal_urls": [],
    "state_urls": [],
    "county_urls": [],
    "municipal_urls": [],
    "tribal_urls": []
}"""

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": "Generate a comprehensive list of official court directory URLs for the United States, including all levels of courts."}
            ],
            response_format={"type": "json_object"}
        )

        result = json.loads(response.choices[0].message.content)
        # Combine all URLs into a single list
        all_urls = (
            result.get('federal_urls', []) + 
            result.get('state_urls', []) + 
            result.get('county_urls', []) +
            result.get('municipal_urls', []) +
            result.get('tribal_urls', [])
        )

        logger.info(f"Found {len(all_urls)} potential court directory URLs")
        return all_urls

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
        # Clean up the URL - remove spaces and parenthetical text
        cleaned_url = url.split('(')[0].strip()
        if not cleaned_url.startswith(('http://', 'https://')):
            cleaned_url = 'https://' + cleaned_url

        if not validate_url(cleaned_url):
            logger.warning(f"Skipping invalid or inaccessible URL: {url}")
            return []

        downloaded = trafilatura.fetch_url(cleaned_url, verify=False)
        if not downloaded:
            logger.warning(f"Failed to download content from {cleaned_url}")
            return []

        content = trafilatura.extract(downloaded, include_links=True, include_tables=True)
        if not content:
            logger.warning(f"No content extracted from {cleaned_url}")
            return []

        courts = discover_courts_from_content(content, cleaned_url)
        logger.info(f"Found {len(courts)} verified courts from {cleaned_url}")
        return courts

    except Exception as e:
        logger.error(f"Error processing court page: {str(e)}")
        return []