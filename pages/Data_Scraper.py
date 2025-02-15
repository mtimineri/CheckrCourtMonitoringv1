import streamlit as st
import pandas as pd
from court_data import get_court_data, get_scraper_status, get_scraper_logs, update_scraper_status
from court_scraper import scrape_courts, update_database, initialize_scraper_run
import time
from datetime import datetime, timedelta
from court_types import federal_courts, state_courts, county_courts
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def format_timestamp(ts):
    """Format timestamp for display"""
    if ts is None:
        return "N/A"
    return pd.to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S")

def get_court_type_status(court_type: str):
    """Get scraper status for specific court type"""
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            return None

        cur = conn.cursor()
        cur.execute("""
            SELECT 
                id, start_time, end_time, total_courts,
                courts_processed, status, message,
                current_court, next_court, stage
            FROM scraper_status
            WHERE court_type = %s
            ORDER BY start_time DESC
            LIMIT 1
        """, (court_type.lower(),))

        status = cur.fetchone()
        if status:
            return {
                'id': status[0],
                'start_time': status[1],
                'end_time': status[2],
                'total_courts': status[3] if status[3] is not None else 0,
                'courts_processed': status[4] if status[4] is not None else 0,
                'status': status[5] if status[5] else 'unknown',
                'message': status[6] if status[6] else '',
                'current_court': status[7] if status[7] else 'None',
                'next_court': status[8] if status[8] else 'None',
                'stage': status[9] if status[9] else 'Not started'
            }
        return None
    except Exception as e:
        logger.error(f"Error in get_court_type_status: {str(e)}")
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# Function to display court tab content
def display_court_tab(court_type: str, get_courts_func):
    """Display controls for a specific court type with improved error handling"""
    try:
        # Get current court data for selection
        conn = get_db_connection()
        if conn is None:
            st.error("Unable to connect to database. Please try again later.")
            return

        courts = []
        try:
            courts = get_courts_func(conn) if conn else []
            if courts is None:
                courts = []
                st.warning(f"No {court_type} courts data available")
        except Exception as e:
            logger.error(f"Error getting courts: {str(e)}")
            st.error(f"Error retrieving {court_type} courts data: {str(e)}")
        finally:
            if conn:
                conn.close()

        col1, col2 = st.columns([2, 1])

        with col1:
            # Only show multiselect if we have courts
            court_names = [court.get('name', '') for court in courts if court.get('name')]
            selected_courts = st.multiselect(
                f"Select specific {court_type} courts to scrape",
                options=court_names,
                help="Leave empty to scrape all courts"
            )

            # Convert selected court names to IDs with null checking
            selected_ids = None
            if selected_courts:
                selected_ids = [
                    court.get('id') for court in courts
                    if court.get('name') in selected_courts and court.get('id') is not None
                ]

            # Check if scraper is running
            try:
                status = get_court_type_status(court_type)
                is_running = status and status.get('status') == 'running'
            except Exception as e:
                logger.error(f"Error checking scraper status: {str(e)}")
                is_running = False

            # Start scraping button
            if st.button(f"Start Scraping {court_type} Courts", disabled=is_running):
                with st.status(f"Scraping {court_type} court data...") as status:
                    try:
                        # Initialize scraper run with proper error handling
                        total_courts = len(selected_courts) if selected_courts else len(courts)
                        if not isinstance(total_courts, int):
                            total_courts = 0
                            st.error("Invalid court count. Please try again.")
                            return

                        if total_courts > 0:
                            run_id = initialize_scraper_run(total_courts)
                            if run_id is not None:
                                status.write(f"Starting scraper for {court_type} courts...")
                                courts_data = scrape_courts(
                                    court_ids=selected_ids,
                                    court_type=court_type.lower()
                                )

                                if courts_data:
                                    status.update(label="Updating database...", state="running")
                                    update_database(courts_data)
                                    status.update(label="Completed!", state="complete")
                                    st.success(f"Successfully scraped {len(courts_data)} courts!")
                                else:
                                    status.update(label="No data collected", state="error")
                                    st.warning("No court data was collected. Please check the logs for details.")
                            else:
                                status.update(label="Failed to initialize scraper", state="error")
                                st.error("Failed to initialize scraper. Please check database connection.")
                        else:
                            status.update(label="No courts to scrape", state="error")
                            st.warning("No courts available to scrape")
                    except Exception as e:
                        error_message = f"Error during scraping: {str(e)}"
                        logger.error(error_message)
                        status.update(label=error_message, state="error")

        # Display current status if available
        current_status = get_court_type_status(court_type)
        if current_status:
            st.subheader(f"{court_type} Courts Status")

            cols = st.columns(3)
            with cols[0]:
                total = current_status.get('total_courts', 0) or 0
                processed = current_status.get('courts_processed', 0) or 0
                progress = (processed / total * 100) if total > 0 else 0
                st.metric("Progress", f"{progress:.1f}%")

            with cols[1]:
                st.metric("Courts Processed", f"{processed}/{total}")

            with cols[2]:
                st.metric("Status", current_status.get('status', 'Unknown').title())

            # Status details
            details_col1, details_col2 = st.columns(2)
            with details_col1:
                st.markdown(f"**Current Court:** {current_status.get('current_court', 'N/A')}")
                st.markdown(f"**Next Court:** {current_status.get('next_court', 'N/A')}")
                st.markdown(f"**Stage:** {current_status.get('stage', 'N/A')}")

            with details_col2:
                st.markdown(f"**Started:** {format_timestamp(current_status.get('start_time'))}")
                st.markdown(f"**Last Updated:** {format_timestamp(current_status.get('end_time'))}")

            if current_status.get('message'):
                st.info(current_status['message'])

            # Auto-refresh while scraper is running
            if current_status.get('status') == 'running':
                time.sleep(2)
                st.rerun()

    except Exception as e:
        logger.error(f"Error in display_court_tab: {str(e)}")
        st.error(f"An error occurred: {str(e)}")

# Page configuration
st.set_page_config(
    page_title="Court Data Scraper | Court Monitoring Platform",
    page_icon="⚖️",
    layout="wide"
)

st.title("Court Data Scraper Control")
st.markdown("Control and monitor the court data scraping process by jurisdiction level")

# Create tabs for different sections
tab1, tab2, tab3, tab4 = st.tabs(["Federal Courts", "State Courts", "County Courts", "Schedule Settings"])

with tab1:
    st.header("Federal Courts")
    display_court_tab("Federal", federal_courts.get_federal_courts)

with tab2:
    st.header("State Courts")
    display_court_tab("State", state_courts.get_state_courts)

with tab3:
    st.header("County Courts")
    display_court_tab("County", county_courts.get_county_courts)

# Schedule Settings Tab
with tab4:
    st.header("Scheduled Scraping")
    schedule_enabled = st.toggle("Enable Scheduled Scraping", value=False)

    if schedule_enabled:
        col1, col2 = st.columns(2)
        with col1:
            court_types = st.multiselect(
                "Select Court Types to Scrape",
                options=["Federal Courts", "State Courts", "County Courts"],
                default=["Federal Courts"]
            )

            frequency = st.selectbox(
                "Scraping Frequency",
                options=["Every 6 hours", "Every 12 hours", "Daily", "Weekly"],
                index=2
            )

        with col2:
            start_time = st.time_input(
                "Start Time (First Run)",
                value=datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            )

        if st.button("Save Schedule"):
            try:
                frequency_hours = {
                    "Every 6 hours": 6,
                    "Every 12 hours": 12,
                    "Daily": 24,
                    "Weekly": 168
                }[frequency]

                st.success(
                    f"Scraper scheduled to run {frequency.lower()} starting at {start_time.strftime('%H:%M')}"
                )

                st.code(f"""Schedule configured:
Court Types: {', '.join(court_types)}
Frequency: {frequency}
Start Time: {start_time.strftime('%H:%M')}
Next Run: {(datetime.now().replace(hour=start_time.hour, minute=start_time.minute) + timedelta(hours=frequency_hours)).strftime('%Y-%m-%d %H:%M')}
""")
            except Exception as e:
                st.error(f"Error setting up schedule: {str(e)}")

# Display scraper logs
with st.expander("Scraper Logs", expanded=True):
    logs = get_scraper_logs()
    if logs:
        log_text = ""
        for log in logs:
            timestamp = format_timestamp(log['timestamp'])
            level = log['level'].upper()
            message = log['message']
            log_text += f"{timestamp} [{level}] {message}\n"
        st.text_area("Latest Logs", log_text, height=300)
    else:
        st.info("No logs available")

def get_db_connection():
    """Get database connection with error handling"""
    try:
        return psycopg2.connect(os.environ['DATABASE_URL'])
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        return None

def return_db_connection(conn):
    try:
        if conn:
            conn.close()
    except Exception as e:
        logger.error(f"Error closing database connection: {str(e)}")

import os
import psycopg2