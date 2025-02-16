import logging
from court_ai_discovery import test_discovery_process
import time
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    """Run test discovery process with improved monitoring"""
    logger.info("Starting court discovery test process")

    start_time = time.time()
    retries = 3
    max_total_time = 900  # 15 minutes maximum total runtime

    for attempt in range(retries):
        try:
            # Check total runtime
            if time.time() - start_time > max_total_time:
                logger.error("Maximum total runtime exceeded")
                return False

            logger.info(f"Starting attempt {attempt + 1}/{retries}")
            success = test_discovery_process()

            if success:
                end_time = time.time()
                duration = end_time - start_time
                logger.info(f"Court discovery test completed successfully in {duration:.2f} seconds")
                return True
            else:
                logger.error(f"Court discovery test failed (attempt {attempt + 1}/{retries})")
                if attempt < retries - 1:
                    wait_time = (attempt + 1) * 5  # Exponential backoff
                    logger.info(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)

        except Exception as e:
            logger.error(f"Error during test (attempt {attempt + 1}/{retries}): {str(e)}")
            if attempt < retries - 1:
                wait_time = (attempt + 1) * 5
                logger.info(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)

    logger.error("All retry attempts failed")
    return False

if __name__ == "__main__":
    try:
        if main():
            sys.exit(0)
        else:
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        sys.exit(1)