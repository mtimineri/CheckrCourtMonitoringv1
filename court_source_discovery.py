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

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_court_url(url: str) -> bool:
    """Validate if a URL is accessible and likely a court website"""
    try:
        response = requests.head(url, timeout=10)
        if response.status_code != 200:
            return False
            
        # Basic validation of domain
        domain = urlparse(url).netloc.lower()
        court_indicators = [
            '.courts.', '.uscourts.', '.court.', 
            'supremecourt', 'judiciary', 'judicial'
        ]
        return any(indicator in domain for indicator in court_indicators)
    except Exception as e:
        logger.error(f"Error validating URL {url}: {str(e)}")
        return False

def discover_court_sources(jurisdiction_type: str) -> List[Dict]:
    """Use AI to discover court directory sources for a given jurisdiction type"""
    try:
        client = OpenAI()
        
        system_prompt = f"""As a court system expert, find official court directory websites for {jurisdiction_type} courts in the United States.

        Return a JSON array of objects with:
        - url: Official website URL
        - jurisdiction_name: Name of the jurisdiction
        - jurisdiction_type: Type of jurisdiction ({jurisdiction_type})
        - source_type: Type of directory (main, regional, specialized)
        
        Focus on:
        1. Official court websites (.gov, .us domains)
        2. State judiciary portals
        3. Federal court directories
        4. Main administrative offices
        
        Exclude:
        1. Third-party legal directories
        2. Law firm websites
        3. Non-governmental organizations
        """

        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Find court directory sources for {jurisdiction_type} courts"}
            ],
            response_format={"type": "json_object"}
        )

        content = response.choices[0].message.content
        sources = json.loads(content).get('sources', [])
        
        # Validate discovered URLs
        validated_sources = []
        for source in sources:
            if validate_court_url(source['url']):
                validated_sources.append(source)
            time.sleep(1)  # Rate limiting
            
        return validated_sources

    except Exception as e:
        logger.error(f"Error discovering court sources: {str(e)}")
        return []

def update_court_sources() -> Dict:
    """Update the database with newly discovered court sources"""
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            return {'status': 'error', 'message': 'Database connection failed'}

        cur = conn.cursor()
        
        # Track statistics
        new_sources = 0
        updated_sources = 0
        
        # Process each jurisdiction type
        jurisdiction_types = ['federal', 'state', 'county', 'municipal']
        
        for jtype in jurisdiction_types:
            logger.info(f"Discovering sources for {jtype} courts")
            sources = discover_court_sources(jtype)
            
            for source in sources:
                try:
                    # First, check if jurisdiction exists
                    cur.execute("""
                        INSERT INTO jurisdictions (name, type)
                        VALUES (%s, %s)
                        ON CONFLICT (name, type) DO UPDATE 
                        SET updated_at = CURRENT_TIMESTAMP
                        RETURNING id;
                    """, (source['jurisdiction_name'], source['jurisdiction_type']))
                    
                    jurisdiction_id = cur.fetchone()[0]
                    
                    # Then, add/update the court source
                    cur.execute("""
                        INSERT INTO court_sources 
                        (jurisdiction_id, source_url, source_type, is_active, last_checked)
                        VALUES (%s, %s, %s, true, CURRENT_TIMESTAMP)
                        ON CONFLICT (jurisdiction_id, source_url) 
                        DO UPDATE SET 
                            source_type = EXCLUDED.source_type,
                            is_active = true,
                            last_checked = CURRENT_TIMESTAMP
                        RETURNING (xmax = 0) as is_insert;
                    """, (jurisdiction_id, source['url'], source['source_type']))
                    
                    is_insert = cur.fetchone()[0]
                    if is_insert:
                        new_sources += 1
                    else:
                        updated_sources += 1
                        
                except Exception as e:
                    logger.error(f"Error adding source {source['url']}: {str(e)}")
                    continue
                    
            conn.commit()
            time.sleep(2)  # Rate limiting between jurisdiction types
            
        cur.close()
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
