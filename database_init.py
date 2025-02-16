import logging
from court_inventory import (
    initialize_database,
    initialize_court_types,
    initialize_jurisdictions,
    initialize_court_sources,
    initialize_base_courts
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        # Initialize database schema
        initialize_database()
        logger.info("Database schema initialized")

        # Initialize court types hierarchy
        initialize_court_types()
        logger.info("Court types initialized")

        # Initialize jurisdictions
        initialize_jurisdictions()
        logger.info("Jurisdictions initialized")

        # Initialize court sources with AI assistance
        initialize_court_sources()
        logger.info("Court sources initialized")

        # Initialize base courts
        initialize_base_courts()
        logger.info("Base courts initialized")

    except Exception as e:
        logger.error(f"Error during initialization: {str(e)}")
        raise

if __name__ == "__main__":
    main()
