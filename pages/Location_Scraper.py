import streamlit as st
import pandas as pd
from court_data import get_db_connection
from datetime import datetime
import time
from court_inventory import update_court_inventory
from court_types import federal_courts, state_courts, county_courts

def format_timestamp(ts):
    """Format timestamp for display"""
    if ts is None:
        return "N/A"
    return pd.to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S")

def get_inventory_status():
    """Get the latest inventory update status"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT 
                id, started_at, completed_at, total_sources,
                sources_processed, new_courts_found, courts_updated,
                status, message, current_court, next_court, stage
            FROM inventory_updates
            ORDER BY started_at DESC
            LIMIT 1
        """)
        status = cur.fetchone()
        return status
    finally:
        cur.close()
        conn.close()

def get_court_sources():
    """Get all court sources with their status"""
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            WITH source_stats AS (
                SELECT 
                    cs.id,
                    COUNT(c.id) as court_count,
                    MAX(c.last_updated) as latest_update
                FROM court_sources cs
                LEFT JOIN courts c ON c.jurisdiction_id = cs.jurisdiction_id
                GROUP BY cs.id
            )
            SELECT 
                cs.id,
                j.name as jurisdiction,
                j.type as jurisdiction_type,
                cs.source_url,
                cs.last_checked,
                cs.last_updated,
                cs.is_active,
                EXTRACT(EPOCH FROM cs.update_frequency)/3600 as update_hours,
                ss.court_count,
                ss.latest_update,
                j.parent_id
            FROM court_sources cs
            JOIN jurisdictions j ON cs.jurisdiction_id = j.id
            LEFT JOIN source_stats ss ON ss.id = cs.id
            ORDER BY j.type, j.name, cs.source_url
        """)
        sources = cur.fetchall()
        return sources
    finally:
        cur.close()
        conn.close()

# Page configuration
st.set_page_config(
    page_title="Court Sources | Court Monitoring Platform",
    page_icon="⚖️",
    layout="wide"
)

st.title("Court Directory Sources")
st.markdown("Monitor and manage court directory sources")

# Add update button and handle update process
col1, col2 = st.columns([2, 1])
with col1:
    update_type = st.selectbox(
        "Select Update Type",
        options=["All Courts", "Federal Courts", "State Courts", "County Courts"],
        key="update_type_select"
    )

    if st.button("Update Court Inventory Now", key="update_inventory_button"):
        try:
            with st.spinner("Updating court inventory..."):
                court_type = update_type.lower().split()[0]
                result = update_court_inventory(court_type=court_type)
                st.success(f"Update completed: Found {result.get('new_courts', 0)} new courts and updated {result.get('updated_courts', 0)} existing courts")
        except Exception as e:
            st.error(f"Error updating inventory: {str(e)}")

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
        if status[9]:  # current_court
            st.markdown(f"**Current Court:** {status[9]}")
        if status[10]:  # next_court
            st.markdown(f"**Next Court:** {status[10]}")
        if status[11]:  # stage
            st.markdown(f"**Stage:** {status[11]}")
        if status[8]:  # message
            st.info(status[8])

    # Auto-refresh while update is running
    if status[7] == 'running':
        time.sleep(2)
        st.rerun()

# Display court sources
st.subheader("Directory Sources")
sources = get_court_sources()

if sources:
    # Group sources by jurisdiction type
    source_data = []
    for source in sources:
        jurisdiction_type = source[2].title()
        # Handle county jurisdictions
        if source[10]:  # If parent_id exists, it's a county
            jurisdiction_type = "County"

        source_data.append({
            'Type': jurisdiction_type,
            'Jurisdiction': source[1],
            'Source URL': source[3],
            'Last Checked': format_timestamp(source[4]),
            'Last Updated': format_timestamp(source[5]),
            'Status': 'Active' if source[6] else 'Inactive',
            'Update Frequency': f"{source[7]:.1f} hours",
            'Courts Tracked': source[8] or 0,
            'Latest Court Update': format_timestamp(source[9])
        })

    source_df = pd.DataFrame(source_data)

    # Add filters
    col1, col2 = st.columns([2, 1])
    with col1:
        selected_types = st.multiselect(
            "Filter by Jurisdiction Type",
            options=sorted(source_df['Type'].unique()),
            default=sorted(source_df['Type'].unique()),
            key="jurisdiction_filter"
        )

    # Filter and display data
    filtered_df = source_df[source_df['Type'].isin(selected_types)]
    st.dataframe(
        filtered_df,
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

3. **County Courts**
   - County Superior Courts
   - County Family Courts
   - County Criminal Courts
   - Specialized County Courts

These sources are regularly checked to:
- Discover new courts
- Update court information
- Maintain accurate court listings
- Track changes in court structure

The system automatically updates this information based on configured frequencies
and maintains a history of all updates for auditing purposes.

You can manually trigger an update using the "Update Court Inventory Now" button at the top of the page.
The update process will:
1. Check all active sources
2. Extract court information
3. Add new courts to the database
4. Update information for existing courts
""")
