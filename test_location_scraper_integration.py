"""Integration test for location scraper workflow"""
import logging
import time
from court_inventory import update_court_inventory, initialize_court_sources
from court_data import get_db_connection
from court_ai_discovery import initialize_ai_discovery
import os

# Ensure logs directory exists
os.makedirs('logs', exist_ok=True)

# Set up logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/location_scraper_test.log')
    ]
)
logger = logging.getLogger(__name__)

def test_location_scraper_integration():
    """Test the entire location scraper workflow"""
    logger.info("Starting location scraper integration test")

    try:
        # Step 1: Initialize court sources
        logger.info("Initializing court sources...")
        initialize_court_sources()

        # Step 2: Initialize AI discovery
        logger.info("Initializing AI discovery...")
        if not initialize_ai_discovery():
            logger.error("Failed to initialize AI discovery")
            return False

        # Step 3: Start the update process with detailed logging
        logger.info("Starting court inventory update...")
        result = update_court_inventory(court_type='federal')

        logger.info(f"Update process initial response: {result}")

        if result.get('status') == 'completed':
            logger.info("Update completed successfully")
            logger.info(f"New courts found: {result.get('new_courts', 0)}")
            logger.info(f"Courts updated: {result.get('updated_courts', 0)}")
            return True
        else:
            logger.error(f"Update failed: {result.get('message', 'Unknown error')}")
            logger.error(f"Status: {result.get('status')}")
            logger.error(f"Stage: {result.get('stage')}")
            return False

    except Exception as e:
        logger.error(f"Test failed with error: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    try:
        success = test_location_scraper_integration()
        if success:
            logger.info("Integration test completed successfully")
            print("Integration test completed successfully")
        else:
            logger.error("Integration test failed")
            print("Integration test failed")
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        print("Test interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        print(f"Unexpected error: {str(e)}")