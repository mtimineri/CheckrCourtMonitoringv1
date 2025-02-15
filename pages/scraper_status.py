import streamlit as st
import pandas as pd
from court_data import get_court_data, get_scraper_status, get_scraper_logs
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
    layout="wide"
)

st.markdown("<h1 class='header'>Court Monitoring Platform</h1>", unsafe_allow_html=True)
st.markdown("### Scraper Status and Court Data")
st.markdown("Monitor court data collection progress and view court information")

# Display scraper status
status = get_scraper_status()

if status:
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Status", status['status'])

    with col2:
        progress = (status['courts_processed'] / status['total_courts'] * 100 
                   if status['total_courts'] else 0)
        st.metric("Progress", f"{progress:.1f}%")

    with col3:
        st.metric("Courts Processed", status['courts_processed'])

    st.text(f"Started: {format_timestamp(status['start_time'])}")
    st.text(f"Last Updated: {format_timestamp(status['end_time'])}")

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
            st.experimental_rerun()

# Display court data table
st.header("Court Data")
df = get_court_data()

if df.empty:
    st.warning("No court data available. Please run the scraper to collect data.")
else:
    # Add search filter
    search = st.text_input("Search courts", "")

    # Apply search filter
    filtered_df = df.copy()
    if search:
        search_mask = pd.Series(False, index=filtered_df.index)
        for col in ['name', 'address']:
            if col in filtered_df.columns:
                search_mask |= filtered_df[col].str.contains(search, case=False, na=False)
        filtered_df = filtered_df[search_mask]

    # Display the table with available columns
    if not filtered_df.empty:
        display_columns = [col for col in ['name', 'type', 'status', 'address'] 
                         if col in filtered_df.columns]
        st.dataframe(
            filtered_df[display_columns],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No courts match the search criteria.")