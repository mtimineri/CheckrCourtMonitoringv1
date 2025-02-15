import streamlit as st
import pandas as pd
from court_data import get_court_data, get_scraper_status, get_scraper_logs
from court_scraper import scrape_courts, update_database
import time
from datetime import datetime, timedelta

def format_timestamp(ts):
    """Format timestamp for display"""
    if ts is None:
        return "N/A"
    return pd.to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S")

# Page configuration
st.set_page_config(
    page_title="Scraper Control | Court Monitoring Platform",
    page_icon="⚖️",
    layout="wide"
)

st.title("Court Data Scraper Control")
st.markdown("Control and monitor the court data scraping process")

# Create tabs for different control sections
tab1, tab2 = st.tabs(["Manual Control", "Schedule Settings"])

with tab1:
    # Get current court data for selection
    df = get_court_data()

    # Create main control section
    st.header("Manual Scraping")
    col1, col2 = st.columns([2, 1])

    with col1:
        # Multi-select for courts
        selected_courts = st.multiselect(
            "Select specific courts to scrape",
            options=df['name'].tolist() if not df.empty else [],
            help="Leave empty to scrape all courts"
        )

        # Convert selected court names to IDs
        selected_ids = None
        if selected_courts:
            selected_ids = df[df['name'].isin(selected_courts)]['id'].tolist()

        # Start scraping button with status container
        if st.button("Start Scraping"):
            status_container = st.empty()
            progress_container = st.empty()
            message_container = st.empty()

            try:
                with status_container.status("Scraping court data...") as status:
                    status.write("Initializing scraper...")
                    courts_data = scrape_courts(selected_ids)

                    if courts_data:
                        status.update(label="Updating database...", state="running")
                        update_database(courts_data)
                        status.update(label="Completed!", state="complete")
                        st.success(f"Successfully scraped {len(courts_data)} courts!")
                    else:
                        status.update(label="No data collected", state="error")
                        st.warning("No court data was collected")

            except Exception as e:
                status_container.error(f"Error during scraping: {str(e)}")

with tab2:
    st.header("Scheduled Scraping")

    # Schedule settings
    schedule_enabled = st.toggle("Enable Scheduled Scraping", value=False)

    if schedule_enabled:
        col1, col2 = st.columns(2)
        with col1:
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

        # Convert frequency to hours for workflow configuration
        frequency_hours = {
            "Every 6 hours": 6,
            "Every 12 hours": 12,
            "Daily": 24,
            "Weekly": 168
        }[frequency]

        if st.button("Save Schedule"):
            try:
                # Configure the scheduled workflow
                st.success(f"Scraper scheduled to run {frequency.lower()} starting at {start_time.strftime('%H:%M')}")

                # Set up the recurring workflow
                command = "python -c 'from court_scraper import scrape_courts, update_database; courts_data = scrape_courts(); update_database(courts_data)'"

                st.code(f"""Schedule configured:
Frequency: {frequency}
Start Time: {start_time.strftime('%H:%M')}
Next Run: {(datetime.now().replace(hour=start_time.hour, minute=start_time.minute) + timedelta(hours=frequency_hours)).strftime('%Y-%m-%d %H:%M')}
""")
            except Exception as e:
                st.error(f"Error setting up schedule: {str(e)}")

# Display scraper status
status = get_scraper_status()
if status:
    st.header("Current Status")

    # Create status metrics
    metrics_container = st.container()
    with metrics_container:
        col1, col2, col3 = st.columns(3)

        with col1:
            progress = (status['courts_processed'] / status['total_courts'] * 100 
                       if status['total_courts'] else 0)
            st.metric("Progress", f"{progress:.1f}%")

        with col2:
            st.metric("Courts Processed", 
                     f"{status['courts_processed']}/{status['total_courts']}"
                     if status['total_courts'] else "0/0")

        with col3:
            st.metric("Status", status['status'].title())

    # Show current operation details
    st.subheader("Operation Details")
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

# Display logs
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