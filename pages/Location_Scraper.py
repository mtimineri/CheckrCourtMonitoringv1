import streamlit as st
import pandas as pd
from court_data import get_db_connection
from datetime import datetime
import time
from court_inventory import update_court_inventory, update_scraper_status
from court_types import federal_courts, state_courts, county_courts
import logging
import os
import psycopg2

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_court_stats():
    """Get current court statistics"""
    conn = get_db_connection()
    if conn is None:
        logger.error("Failed to get database connection")
        return None

    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT 
                type,
                COUNT(*) as count,
                MAX(last_updated) as latest_update,
                COUNT(CASE WHEN status = 'Open' THEN 1 END) as open_courts,
                COUNT(CASE WHEN status = 'Closed' THEN 1 END) as closed_courts,
                COUNT(CASE WHEN status = 'Limited Operations' THEN 1 END) as limited_courts
            FROM courts
            GROUP BY type
            ORDER BY type;
        """)
        return cur.fetchall()
    except Exception as e:
        logger.error(f"Error getting court stats: {str(e)}")
        return None
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def get_court_sources():
    """Get all court sources with their status"""
    try:
        conn = get_db_connection()
        if conn is None:
            logger.error("Failed to get database connection")
            st.error("Unable to connect to database. Please try again later.")
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
                WHERE cs.is_active = true
                ORDER BY j.type, j.name, cs.source_url;
            """)
            sources = cur.fetchall()
            logger.info(f"Retrieved {len(sources)} active court sources")
            return sources
        except Exception as e:
            logger.error(f"Error querying court sources: {str(e)}")
            st.error("Error retrieving court sources. Please try again later.")
            return []
        finally:
            cur.close()
            if conn:
                conn.close()
    except Exception as e:
        logger.error(f"Error in get_court_sources: {str(e)}")
        st.error("An unexpected error occurred. Please try again later.")
        return []

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
                id, started_at, completed_at, total_sources,
                sources_processed, status, message,
                current_source, next_source, stage, new_courts_found, courts_updated
            FROM inventory_updates
            WHERE status != 'completed'
            ORDER BY started_at DESC
            LIMIT 1
        """)
        status = cur.fetchone()
        if status:
            return {
                'id': status[0],
                'start_time': status[1],
                'end_time': status[2],
                'total_sources': status[3] if status[3] is not None else 0,
                'sources_processed': status[4] if status[4] is not None else 0,
                'status': status[5] if status[5] else 'unknown',
                'message': status[6] if status[6] else '',
                'current_source': status[7] if status[7] else 'None',
                'next_source': status[8] if status[8] else 'None',
                'stage': status[9] if status[9] else 'Not started',
                'new_courts_found': status[10] if status[10] is not None else 0,
                'courts_updated': status[11] if status[11] is not None else 0
            }

        # If no running status found, get the last completed one
        cur.execute("""
            SELECT 
                id, started_at, completed_at, total_sources,
                sources_processed, status, message,
                current_source, next_source, stage, new_courts_found, courts_updated
            FROM inventory_updates
            ORDER BY started_at DESC
            LIMIT 1
        """)
        status = cur.fetchone()
        if status:
            return {
                'id': status[0],
                'start_time': status[1],
                'end_time': status[2],
                'total_sources': status[3] if status[3] is not None else 0,
                'sources_processed': status[4] if status[4] is not None else 0,
                'status': status[5] if status[5] else 'unknown',
                'message': status[6] if status[6] else '',
                'current_source': status[7] if status[7] else 'None',
                'next_source': status[8] if status[8] else 'None',
                'stage': status[9] if status[9] else 'Not started',
                'new_courts_found': status[10] if status[10] is not None else 0,
                'courts_updated': status[11] if status[11] is not None else 0
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

# Page configuration
st.set_page_config(
    page_title="Court Location Scraper | Court Monitoring Platform",
    page_icon="⚖️",
    layout="wide"
)

st.title("Court Location Scraper")
st.markdown("Monitor and update the court location inventory")

# Display court statistics
stats = get_court_stats()
if stats:
    st.subheader("Current Court Statistics")

    # Create a DataFrame for better display
    stats_df = pd.DataFrame(stats, columns=[
        'Court Type', 'Total Courts', 'Last Updated',
        'Open Courts', 'Closed Courts', 'Limited Operations'
    ])

    # Display statistics in columns
    col1, col2, col3 = st.columns(3)

    with col1:
        total_courts = stats_df['Total Courts'].sum()
        st.metric("Total Courts", f"{total_courts:,}")

    with col2:
        open_courts = stats_df['Open Courts'].sum()
        st.metric("Open Courts", f"{open_courts:,} ({open_courts/total_courts*100:.1f}%)")

    with col3:
        latest_update = stats_df['Last Updated'].max()
        st.metric("Latest Update", format_timestamp(latest_update))

    # Display detailed statistics by court type
    st.dataframe(
        stats_df.style.format({
            'Total Courts': '{:,}',
            'Open Courts': '{:,}',
            'Closed Courts': '{:,}',
            'Limited Operations': '{:,}',
            'Last Updated': lambda x: format_timestamp(x)
        }),
        use_container_width=True,
        hide_index=True
    )

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
                    # Get updated statistics
                    new_stats = get_court_stats()
                    if new_stats:
                        new_stats_df = pd.DataFrame(new_stats, columns=[
                            'Court Type', 'Total Courts', 'Last Updated',
                            'Open Courts', 'Closed Courts', 'Limited Operations'
                        ])

                        st.success(f"""
                            Update completed successfully:
                            - Found {result.get('new_courts', 0)} new courts
                            - Updated {result.get('updated_courts', 0)} existing courts
                            - Total courts: {new_stats_df['Total Courts'].sum():,}
                            - Latest update: {format_timestamp(new_stats_df['Last Updated'].max())}
                        """)
                    else:
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

    # Add additional metrics for better visibility
    col1, col2 = st.columns(2)
    with col1:
        st.metric("New Courts Found", status.get('new_courts_found', 0))
        st.metric("Courts Updated", status.get('courts_updated', 0))

    with col2:
        conn = get_db_connection()
        cur = conn.cursor()
        # Get the last successful update timestamp
        cur.execute("""
            SELECT completed_at 
            FROM inventory_updates 
            WHERE status = 'completed' 
            ORDER BY completed_at DESC 
            LIMIT 1
        """)
        last_update = cur.fetchone()
        if last_update:
            st.metric("Last Successful Update", format_timestamp(last_update[0]))

        # Get total courts count
        cur.execute("SELECT COUNT(*) FROM courts")
        total_courts = cur.fetchone()[0]
        st.metric("Total Courts in Database", total_courts)
        cur.close()
        conn.close()


    # Update status details section
    status_details = [
        ("Status", status.get('status', 'Unknown').title()),
        ("Started", format_timestamp(status.get('start_time'))),
        ("Completed", format_timestamp(status.get('end_time'))),
        ("Current Source", status.get('current_source', 'None')),
        ("Next Source", status.get('next_source', 'None')),
        ("Stage", status.get('stage', 'Not started')),
    ]

    st.subheader("Update Details")
    for label, value in status_details:
        st.text(f"{label}: {value}")

    if status.get('message'):
        st.info(status.get('message'))

    # Add error logs if any
    conn = get_db_connection()
    cur = conn.cursor()
    if status.get('status') == 'error':
        st.error("Latest Error Logs:")
        cur.execute("""
            SELECT timestamp, message 
            FROM scraper_logs 
            WHERE scraper_run_id = %s 
            AND level = 'ERROR'
            ORDER BY timestamp DESC 
            LIMIT 5
        """, (status['id'],))
        error_logs = cur.fetchall()
        for timestamp, message in error_logs:
            st.code(f"{format_timestamp(timestamp)}: {message}")
    cur.close()
    conn.close()

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