import streamlit as st
import pandas as pd
from court_data import get_db_connection
from datetime import datetime

def format_timestamp(ts):
    """Format timestamp for display"""
    if ts is None:
        return "N/A"
    return pd.to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S")

def get_inventory_status():
    """Get the latest inventory update status"""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            id, started_at, completed_at, total_sources,
            sources_processed, new_courts_found, courts_updated,
            status, message
        FROM inventory_updates
        ORDER BY started_at DESC
        LIMIT 1
    """)
    status = cur.fetchone()

    cur.close()
    conn.close()
    return status

def get_court_sources():
    """Get all court sources with their status"""
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            cs.id,
            j.name as jurisdiction,
            cs.source_url,
            cs.last_checked,
            cs.last_updated,
            cs.is_active,
            EXTRACT(EPOCH FROM cs.update_frequency)/3600 as update_hours
        FROM court_sources cs
        JOIN jurisdictions j ON cs.jurisdiction_id = j.id
        ORDER BY j.name, cs.source_url
    """)
    sources = cur.fetchall()

    cur.close()
    conn.close()
    return sources

# Page configuration
st.set_page_config(
    page_title="Court Sources | Court Monitoring Platform",
    page_icon="⚖️",
    layout="wide"
)

st.title("Court Directory Sources")
st.markdown("Monitor and manage court directory sources")

# Get current status
status = get_inventory_status()
if status:
    # Create metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        progress = (status[4] / status[3] * 100 if status[3] else 0)
        st.metric("Update Progress", f"{progress:.1f}%")
    
    with col2:
        st.metric("New Courts Found", status[5] or 0)
    
    with col3:
        st.metric("Courts Updated", status[6] or 0)

    # Status details
    st.subheader("Latest Update Status")
    status_cols = st.columns(2)
    
    with status_cols[0]:
        st.markdown(f"**Status:** {status[7].title()}")
        st.markdown(f"**Started:** {format_timestamp(status[1])}")
        st.markdown(f"**Completed:** {format_timestamp(status[2])}")
    
    with status_cols[1]:
        st.markdown(f"**Sources:** {status[4]}/{status[3]}")
        if status[8]:  # message
            st.info(status[8])

# Display court sources
st.subheader("Directory Sources")
sources = get_court_sources()

if sources:
    source_data = []
    for source in sources:
        source_data.append({
            'Jurisdiction': source[1],
            'Source URL': source[2],
            'Last Checked': format_timestamp(source[3]),
            'Last Updated': format_timestamp(source[4]),
            'Status': 'Active' if source[5] else 'Inactive',
            'Update Frequency': f"{source[6]:.1f} hours"
        })
    
    source_df = pd.DataFrame(source_data)
    st.dataframe(
        source_df,
        use_container_width=True,
        hide_index=True
    )
else:
    st.info("No court sources configured")

# Add explanatory text
st.markdown("""
### About Court Directory Sources

The court monitoring platform maintains a list of authoritative sources for court information:

1. **Federal Courts**
   - Supreme Court website
   - U.S. Courts directory
   - Circuit Court websites
   - District Court listings
   - Bankruptcy Court directory

2. **State Courts**
   - State judiciary portals
   - State government court directories
   - State-specific court locators

These sources are regularly checked to:
- Discover new courts
- Update court information
- Maintain accurate court listings
- Track changes in court structure

The system automatically updates this information based on configured frequencies
and maintains a history of all updates for auditing purposes.
""")
