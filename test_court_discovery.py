import logging
from court_ai_discovery import test_discovery_process

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    """Run test discovery process"""
    logger.info("Starting court discovery test process")
    
    success = test_discovery_process()
    
    if success:
        logger.info("Court discovery test completed successfully")
    else:
        logger.error("Court discovery test failed")

if __name__ == "__main__":
    main()
