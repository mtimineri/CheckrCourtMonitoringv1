import streamlit as st
import pandas as pd
from court_data import get_court_data, get_scraper_status

def format_timestamp(ts):
    """Format timestamp for display"""
    if ts is None:
        return "N/A"
    return pd.to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S")

def render_dashboard():
    st.title("Court Data Dashboard")

    # Display scraper status
    st.header("Scraper Status")
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

    # Display court data table
    st.header("Court Data")
    df = get_court_data()
    
    # Add filters
    col1, col2 = st.columns(2)
    with col1:
        search = st.text_input("Search courts", "")
    with col2:
        status_filter = st.multiselect(
            "Filter by status",
            options=df['status'].unique(),
            default=df['status'].unique()
        )
    
    # Apply filters
    mask = df['status'].isin(status_filter)
    if search:
        mask &= (
            df['name'].str.contains(search, case=False) |
            df['address'].str.contains(search, case=False)
        )
    
    filtered_df = df[mask].copy()
    
    # Format last_updated column
    filtered_df['last_updated'] = pd.to_datetime(filtered_df['last_updated']).dt.strftime("%Y-%m-%d %H:%M:%S")
    
    # Display the table
    st.dataframe(
        filtered_df[[
            'name', 'type', 'status', 'address', 'last_updated'
        ]],
        use_container_width=True,
        hide_index=True
    )

if __name__ == "__main__":
    render_dashboard()
