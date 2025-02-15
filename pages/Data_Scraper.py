import streamlit as st
import pandas as pd
from court_data import get_court_data, get_scraper_status, get_scraper_logs
from court_scraper import scrape_courts, update_database, initialize_scraper_run
import time
from datetime import datetime, timedelta
from court_types import federal_courts, state_courts, county_courts
import psycopg2
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def format_timestamp(ts):
    """Format timestamp for display"""
    if ts is None:
        return "N/A"
    return pd.to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S")

def get_db_connection():
    """Get database connection with error handling"""
    try:
        return psycopg2.connect(os.environ['DATABASE_URL'])
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        return None

def get_court_type_status(court_type: str):
    """Get scraper status for specific court type"""
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            logger.error("Database connection failed in get_court_type_status")
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
            # Convert None values to 0 for numeric fields
            total_courts = status[3] if status[3] is not None else 0
            courts_processed = status[4] if status[4] is not None else 0

            return {
                'id': status[0],
                'start_time': status[1],
                'end_time': status[2],
                'total_courts': total_courts,
                'courts_processed': courts_processed,
                'status': status[5] if status[5] else 'unknown',
                'message': status[6] if status[6] else '',
                'current_court': status[7] if status[7] else 'None',
                'next_court': status[8] if status[8] else 'None',
                'stage': status[9] if status[9] else 'Not started'
            }
        return None
    except Exception as e:
        logger.error(f"Database error in get_court_type_status: {str(e)}")
        st.error(f"Database error in get_court_type_status: {str(e)}")
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def display_court_tab(court_type: str, get_courts_func):
    """Display controls for a specific court type"""
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            st.error("Unable to connect to database. Please try again later.")
            return

        # Get current court data for selection
        try:
            courts = get_courts_func(conn) or []
        except Exception as e:
            logger.error(f"Error getting courts data: {str(e)}")
            courts = []

        col1, col2 = st.columns([2, 1])

        with col1:
            selected_courts = st.multiselect(
                f"Select specific {court_type} courts to scrape",
                options=[court['name'] for court in courts] if courts else [],
                help="Leave empty to scrape all courts"
            )

            # Convert selected court names to IDs
            selected_ids = None
            if selected_courts:
                selected_ids = [
                    court['id'] for court in courts
                    if court['name'] in selected_courts
                ]

            # Check if any scraper is running for this court type
            try:
                current_status = get_court_type_status(court_type)
                is_running = current_status and current_status.get('status') == 'running'
            except Exception as e:
                logger.error(f"Error getting court status: {str(e)}")
                current_status = None
                is_running = False

            # Start scraping button
            if st.button(f"Start Scraping {court_type} Courts", disabled=is_running):
                status_container = st.empty()
                progress_container = st.empty()
                message_container = st.empty()

                try:
                    with status_container.status(f"Scraping {court_type} court data...") as status:
                        # Initialize scraper run with proper total courts count
                        total_courts = len(selected_courts) if selected_courts else (len(courts) if courts else 0)

                        if total_courts > 0:
                            run_id = initialize_scraper_run(total_courts)
                            if run_id is None:
                                status.update(label="Failed to initialize scraper", state="error")
                                st.error("Failed to initialize the scraper. Please try again.")
                                return

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
                                st.warning("No court data was collected")
                        else:
                            status.update(label="No courts to scrape", state="error")
                            st.warning("No courts available to scrape")

                except Exception as e:
                    status_container.error(f"Error during scraping: {str(e)}")

        # Display court type specific status
        if current_status:
            st.subheader(f"{court_type} Courts Status")

            # Create metrics with safe calculations
            metric_cols = st.columns(3)
            with metric_cols[0]:
                # Safely calculate progress
                total = current_status.get('total_courts', 0)
                processed = current_status.get('courts_processed', 0)
                progress = (processed / total * 100) if total > 0 else 0
                st.metric("Progress", f"{progress:.1f}%")

            with metric_cols[1]:
                st.metric(
                    "Courts Processed",
                    f"{processed}/{total}"
                )

            with metric_cols[2]:
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
        st.error(f"Error accessing court data: {str(e)}")
    finally:
        if conn:
            conn.close()

# Page configuration
st.set_page_config(
    page_title="Scraper Control | Court Monitoring Platform",
    page_icon="⚖️",
    layout="wide"
)

st.title("Court Data Scraper Control")
st.markdown("Control and monitor the court data scraping process by jurisdiction level")

# Create tabs for different control sections
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

# Display scraper logs in expandable section
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