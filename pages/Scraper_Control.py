import streamlit as st
import pandas as pd
from court_data import get_court_data, get_scraper_status, get_scraper_logs
from court_scraper import scrape_courts, update_database
import time

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

# Get current court data for selection
df = get_court_data()

# Create main control section
st.header("Scraping Control")
col1, col2 = st.columns([2, 1])

with col1:
    # Multi-select for courts
    selected_courts = st.multiselect(
        "Select specific courts to scrape",
        options=df['name'].tolist(),
        help="Leave empty to scrape all courts"
    )

    # Convert selected court names to IDs
    selected_ids = None
    if selected_courts:
        selected_ids = df[df['name'].isin(selected_courts)]['id'].tolist()

    # Start scraping button
    if st.button("Start Scraping"):
        try:
            with st.spinner("Scraping court data..."):
                courts_data = scrape_courts(selected_ids)
                if courts_data:
                    update_database(courts_data)
                    st.success(f"Successfully scraped {len(courts_data)} courts!")
                else:
                    st.warning("No court data was collected")
        except Exception as e:
            st.error(f"Error during scraping: {str(e)}")

# Display scraper status
status = get_scraper_status()
if status:
    st.header("Current Status")
    
    # Create metrics
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
        st.markdown(f"**Current Court:** {status['current_court']}")
        st.markdown(f"**Next Court:** {status['next_court']}")
        st.markdown(f"**Stage:** {status['stage']}")
    
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

# Add explanatory text
st.markdown("""
### About the Court Data Scraper

The court data scraper collects information about court operations and status:

1. **Full Scrape**
   - Leave the court selection empty to scrape all courts in the inventory
   - This will take longer but provides a complete update

2. **Selective Scrape**
   - Select specific courts to update only their information
   - Useful for checking specific courts of interest
   - Faster than a full scrape

3. **Data Collection**
   - The scraper visits each court's website
   - Extracts current operational status
   - Updates location and contact information
   - Records any changes in the database

4. **Monitoring**
   - View real-time progress above
   - Check the logs for detailed information
   - Status updates automatically while scraper is running
""")
