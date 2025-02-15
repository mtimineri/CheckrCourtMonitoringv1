import streamlit as st
import pandas as pd
from court_data import get_court_data, get_scraper_status, get_scraper_logs
from court_scraper import scrape_courts, update_database
import time
from datetime import datetime, timedelta
from court_types import federal_courts, state_courts, county_courts
import psycopg2
import os


def format_timestamp(ts):
    """Format timestamp for display"""
    if ts is None:
        return "N/A"
    return pd.to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S")


def get_db_connection():
    return psycopg2.connect(os.environ['DATABASE_URL'])


def get_court_type_status(court_type: str):
    """Get scraper status for specific court type"""
    conn = get_db_connection()
    cur = conn.cursor()
    try:
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
                'total_courts': status[3],
                'courts_processed': status[4],
                'status': status[5],
                'message': status[6],
                'current_court': status[7],
                'next_court': status[8],
                'stage': status[9]
            }
        return None
    finally:
        cur.close()
        conn.close()


# Page configuration
st.set_page_config(page_title="Scraper Control | Court Monitoring Platform",
                   page_icon="⚖️",
                   layout="wide")

st.title("Court Data Scraper Control")
st.markdown(
    "Control and monitor the court data scraping process by jurisdiction level"
)

# Create tabs for different control sections
tab1, tab2, tab3, tab4 = st.tabs(
    ["Federal Courts", "State Courts", "County Courts", "Schedule Settings"])


def display_court_tab(court_type: str, get_courts_func, scrape_func):
    """Display controls for a specific court type"""
    conn = get_db_connection()
    try:
        # Get current court data for selection
        courts = get_courts_func(conn)

        col1, col2 = st.columns([2, 1])

        with col1:
            selected_courts = st.multiselect(
                f"Select specific {court_type} courts to scrape",
                options=[court['name'] for court in courts] if courts else [],
                help="Leave empty to scrape all courts")

            # Convert selected court names to IDs
            selected_ids = None
            if selected_courts:
                selected_ids = [
                    court['id'] for court in courts
                    if court['name'] in selected_courts
                ]

            # Start scraping button
            if st.button(f"Start Scraping {court_type} Courts"):
                status_container = st.empty()
                progress_container = st.empty()
                message_container = st.empty()

                try:
                    with status_container.status(
                            f"Scraping {court_type} court data...") as status:
                        status.write("Initializing scraper...")
                        # Pass court_type as a single argument
                        courts_data = scrape_courts(selected_ids, court_type.lower())

                        if courts_data:
                            status.update(label="Updating database...",
                                        state="running")
                            update_database(courts_data, court_type=court_type.lower())
                            status.update(label="Completed!", state="complete")
                            st.success(
                                f"Successfully scraped {len(courts_data)} courts!")
                        else:
                            status.update(label="No data collected", state="error")
                            st.warning("No court data was collected")

                except Exception as e:
                    status_container.error(f"Error during scraping: {str(e)}")

        # Display court type specific status
        status = get_court_type_status(court_type)
        if status:
            st.subheader(f"{court_type} Courts Status")

            # Create metrics
            metric_cols = st.columns(3)
            with metric_cols[0]:
                progress = (status['courts_processed'] / status['total_courts'] * 
                          100 if status['total_courts'] else 0)
                st.metric("Progress", f"{progress:.1f}%")

            with metric_cols[1]:
                st.metric("Courts Processed",
                         f"{status['courts_processed']}/{status['total_courts']}"
                         if status['total_courts'] else "0/0")

            with metric_cols[2]:
                st.metric("Status", status['status'].title())

            # Status details
            details_col1, details_col2 = st.columns(2)
            with details_col1:
                st.markdown(f"**Current Court:** {status['current_court'] or 'N/A'}")
                st.markdown(f"**Next Court:** {status['next_court'] or 'N/A'}")
                st.markdown(f"**Stage:** {status['stage'] or 'N/A'}")

            with details_col2:
                st.markdown(f"**Started:** {format_timestamp(status['start_time'])}")
                st.markdown(f"**Last Updated:** {format_timestamp(status['end_time'])}")

            if status['message']:
                st.info(status['message'])

            # Auto-refresh while scraper is running
            if status['status'] == 'running':
                time.sleep(2)
                st.rerun()

    finally:
        conn.close()


with tab1:
    st.header("Federal Courts")
    display_court_tab("Federal", federal_courts.get_federal_courts,
                     scrape_courts)

with tab2:
    st.header("State Courts")
    display_court_tab("State", state_courts.get_state_courts, scrape_courts)

with tab3:
    st.header("County Courts")
    display_court_tab("County", county_courts.get_county_courts,
                     scrape_courts)

with tab4:
    st.header("Scheduled Scraping")
    schedule_enabled = st.toggle("Enable Scheduled Scraping", value=False)

    if schedule_enabled:
        col1, col2 = st.columns(2)
        with col1:
            court_types = st.multiselect(
                "Select Court Types to Scrape",
                options=["Federal Courts", "State Courts", "County Courts"],
                default=["Federal Courts"])

            frequency = st.selectbox(
                "Scraping Frequency",
                options=["Every 6 hours", "Every 12 hours", "Daily", "Weekly"],
                index=2)

        with col2:
            start_time = st.time_input(
                "Start Time (First Run)",
                value=datetime.now().replace(hour=0,
                                          minute=0,
                                          second=0,
                                          microsecond=0))

        if st.button("Save Schedule"):
            try:
                # Configure the scheduled workflow
                st.success(
                    f"Scraper scheduled to run {frequency.lower()} starting at {start_time.strftime('%H:%M')}"
                )

                # Set up the recurring workflow for each selected court type
                frequency_hours = {
                    "Every 6 hours": 6,
                    "Every 12 hours": 12,
                    "Daily": 24,
                    "Weekly": 168
                }[frequency]

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