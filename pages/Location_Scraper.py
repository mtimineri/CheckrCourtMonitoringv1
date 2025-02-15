import streamlit as st
import pandas as pd
from court_data import get_db_connection
from datetime import datetime
import time
from court_inventory import update_court_inventory, update_scraper_status
from court_types import federal_courts, state_courts, county_courts

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def format_timestamp(ts):
    """Format timestamp for display"""
    if ts is None:
        return "N/A"
    return pd.to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S")

def get_inventory_status():
    """Get the latest inventory update status"""
    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to get database connection")
        return None

    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT 
                id, start_time, end_time, total_sources,
                sources_processed, status, message,
                current_source, next_source, stage
            FROM inventory_updates
            ORDER BY start_time DESC
            LIMIT 1
        """)
        status = cur.fetchone()
        if status:
            return {
                'id': status[0],
                'start_time': status[1],
                'end_time': status[2],
                'total_sources': status[3],
                'sources_processed': status[4],
                'status': status[5],
                'message': status[6],
                'current_source': status[7],
                'next_source': status[8],
                'stage': status[9]
            }
        return None
    except Exception as e:
        logger.error(f"Error getting inventory status: {str(e)}")
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def return_db_connection(conn):
    try:
        if conn:
            conn.close()
    except Exception as e:
        logger.error(f"Error closing database connection: {str(e)}")

# Page configuration
st.set_page_config(
    page_title="Court Location Scraper | Court Monitoring Platform",
    page_icon="⚖️",
    layout="wide"
)

st.title("Court Location Scraper")
st.markdown("Monitor and update the court location inventory")

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
            with st.status("Updating court inventory...") as status:
                court_type = update_type.lower().split()[0]
                result = update_court_inventory(court_type=court_type)
                if result.get('status') != 'error':
                    st.success(f"Update completed: Found {result.get('new_courts', 0)} new courts and updated {result.get('updated_courts', 0)} existing courts")
                else:
                    st.error(f"Error updating inventory: {result.get('message')}")
        except Exception as e:
            st.error(f"Error updating inventory: {str(e)}")

# Get current status
status = get_inventory_status()
if status:
    # Create metrics
    col1, col2, col3 = st.columns(3)

    with col1:
        total = status.get('total_sources', 0) or 0
        processed = status.get('sources_processed', 0) or 0
        progress = (processed / total * 100) if total > 0 else 0
        st.metric("Update Progress", f"{progress:.1f}%")

    with col2:
        st.metric("Sources Processed", f"{processed}/{total}")

    with col3:
        st.metric("Status", status.get('status', 'Unknown').title())

    # Status details
    st.subheader("Latest Update Status")
    status_cols = st.columns(2)

    with status_cols[0]:
        st.markdown(f"**Status:** {status.get('status', 'Unknown').title()}")
        st.markdown(f"**Started:** {format_timestamp(status.get('start_time'))}")
        st.markdown(f"**Completed:** {format_timestamp(status.get('end_time'))}")

    with status_cols[1]:
        st.markdown(f"**Current Source:** {status.get('current_source', 'None')}")
        st.markdown(f"**Next Source:** {status.get('next_source', 'None')}")
        st.markdown(f"**Stage:** {status.get('stage', 'Not started')}")

        if status.get('message'):
            st.info(status.get('message'))

    # Auto-refresh while update is running
    if status.get('status') == 'running':
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

def get_court_sources():
    """Get all court sources with their status"""
    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to get database connection")
        return []

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
    except Exception as e:
        logger.error(f"Error getting court sources: {str(e)}")
        return []
    finally:
        return_db_connection(cur)
        return_db_connection(conn)