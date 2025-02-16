"""Integration test for court source discovery process"""
import logging
import os
from court_source_discovery import update_court_sources
from court_data import get_db_connection

# Ensure logs directory exists
os.makedirs('logs', exist_ok=True)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/court_source_discovery_test.log')
    ]
)
logger = logging.getLogger(__name__)

def test_court_source_discovery():
    """Test the court source discovery process"""
    try:
        logger.info("Starting court source discovery test")
        
        # Get initial source count
        conn = get_db_connection()
        cur = conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM court_sources")
        initial_count = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        # Run the update process
        result = update_court_sources()
        
        if result['status'] == 'completed':
            logger.info(f"Discovery completed successfully")
            logger.info(f"New sources added: {result['new_sources']}")
            logger.info(f"Sources updated: {result['updated_sources']}")
            
            # Verify the changes
            conn = get_db_connection()
            cur = conn.cursor()
            
            cur.execute("SELECT COUNT(*) FROM court_sources")
            final_count = cur.fetchone()[0]
            
            cur.close()
            conn.close()
            
            logger.info(f"Total sources: {final_count} (was {initial_count})")
            
            return True
        else:
            logger.error(f"Discovery failed: {result.get('message', 'Unknown error')}")
            return False
            
    except Exception as e:
        logger.error(f"Test failed with error: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    try:
        success = test_court_source_discovery()
        if success:
            logger.info("Discovery test completed successfully")
            print("Discovery test completed successfully")
        else:
            logger.error("Discovery test failed")
            print("Discovery test failed")
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        print("Test interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        print(f"Unexpected error: {str(e)}")
