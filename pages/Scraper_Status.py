import streamlit as st
import pandas as pd
from court_data import get_scraper_status, get_scraper_logs
import time

def format_timestamp(ts):
    """Format timestamp for display"""
    if ts is None:
        return "N/A"
    return pd.to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S")

# Page configuration
st.set_page_config(
    page_title="Scraper Status | Court Monitoring Platform",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.header("Scraper Status")
st.markdown("Monitor court data collection progress and view scraper logs")

# Display scraper status
status = get_scraper_status()

if status:
    # Create three columns for metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Status", status['status'].title())

    with col2:
        progress = (status['courts_processed'] / status['total_courts'] * 100 
                   if status['total_courts'] else 0)
        st.metric("Progress", f"{progress:.1f}%")

    with col3:
        st.metric("Courts Processed", 
                 f"{status['courts_processed']}/{status['total_courts']}" 
                 if status['total_courts'] else "0/0")

    # Display detailed status information
    st.subheader("Current Progress")
    status_col1, status_col2 = st.columns(2)

    with status_col1:
        st.markdown(f"**Current Stage:** {status.get('stage', 'N/A')}")
        st.markdown(f"**Current Court:** {status.get('current_court', 'N/A')}")
        st.markdown(f"**Next Court:** {status.get('next_court', 'N/A')}")

    with status_col2:
        st.markdown(f"**Started:** {format_timestamp(status['start_time'])}")
        st.markdown(f"**Last Updated:** {format_timestamp(status['end_time'])}")

    if status['message']:
        st.info(status['message'])

    # Add expandable logs section
    with st.expander("Scraper Logs", expanded=True):
        logs_placeholder = st.empty()

        def display_logs():
            logs = get_scraper_logs()
            if logs:
                log_text = ""
                for log in logs:
                    timestamp = format_timestamp(log['timestamp'])
                    level = log['level'].upper()
                    message = log['message']
                    log_text += f"{timestamp} [{level}] {message}\n"
                logs_placeholder.text_area("Latest Logs", log_text, height=300)
            else:
                logs_placeholder.info("No logs available")

        display_logs()

        if status['status'] == 'running':
            st.markdown("*Logs auto-refresh every 5 seconds while scraper is running*")
            time.sleep(5)
            st.rerun()