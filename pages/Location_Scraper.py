import streamlit as st
import pandas as pd
from datetime import datetime
import time
from court_inventory import update_court_inventory, update_scraper_status, initialize_court_sources
from court_ai_discovery import initialize_ai_discovery
import logging
import os
import psycopg2
from court_data import get_db_connection, get_court_types, get_court_statuses

# Set up logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Initialize court sources and AI discovery if needed
if 'sources_initialized' not in st.session_state:
    try:
        initialize_court_sources()
        initialize_ai_discovery()
        st.session_state.sources_initialized = True
        logger.info("Court sources initialized successfully")
    except Exception as e:
        logger.error(f"Error initializing court sources: {str(e)}")
        st.error("Failed to initialize court sources. Please try again.")

def format_timestamp(ts):
    """Format timestamp for display"""
    if ts is None:
        return "N/A"
    return pd.to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S")

# Page configuration
st.set_page_config(
    page_title="Court Location Scraper | Court Monitoring Platform",
    page_icon="⚖️",
    layout="wide"
)

st.title("Court Location Scraper")
st.markdown("Monitor and update the court location inventory")

# Create a placeholder for the status section that will update automatically
status_placeholder = st.empty()

def display_status_section():
    """Display the status section with current information"""
    status = get_inventory_status()
    if status and status.get('status') == 'running':
        with status_placeholder.container():
            st.subheader("Current Update Status")

            # Create metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                total = status.get('total_sources', 0)
                processed = status.get('sources_processed', 0)
                progress = (processed / total * 100) if total > 0 else 0
                st.metric(
                    "Update Progress",
                    f"{progress:.1f}%",
                    delta=f"{processed} of {total} sources"
                )

            with col2:
                st.metric(
                    "Status",
                    status.get('status', 'Unknown').title(),
                    delta=status.get('stage', '')
                )

            with col3:
                st.metric(
                    "Courts Found",
                    status.get('new_courts_found', 0),
                    delta=f"+{status.get('courts_updated', 0)} updated"
                )

            # Show current activity
            if status.get('current_source'):
                st.info(f"Currently processing: {status.get('current_source', 'Unknown')}")
            if status.get('message'):
                st.write(status.get('message'))

    else:
        # Show regular status display for non-running states
        with status_placeholder.container():
            if status:
                st.subheader("Last Update Status")

                # Create metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    total = status.get('total_sources', 0)
                    processed = status.get('sources_processed', 0)
                    progress = (processed / total * 100) if total > 0 else 0
                    st.metric(
                        "Update Progress",
                        f"{progress:.1f}%",
                        delta=f"{processed} of {total} sources"
                    )

                with col2:
                    st.metric(
                        "Status",
                        status.get('status', 'Unknown').title(),
                        delta=status.get('stage', '')
                    )

                with col3:
                    st.metric(
                        "Courts Found",
                        status.get('new_courts_found', 0),
                        delta=f"+{status.get('courts_updated', 0)} updated"
                    )

def get_inventory_status():
    """Get the latest inventory update status"""
    conn = None
    try:
        conn = get_db_connection()
        if conn is None:
            logger.error("Failed to get database connection")
            st.error("Database connection error. Please try again later.")
            return None

        cur = conn.cursor()
        try:
            # First check for any running updates
            cur.execute("""
                SELECT 
                    id, started_at, completed_at, total_sources,
                    sources_processed, status, message,
                    current_source, next_source, stage,
                    new_courts_found, courts_updated
                FROM inventory_updates
                WHERE status = 'running'
                ORDER BY started_at DESC
                LIMIT 1
            """)
            status = cur.fetchone()

            if not status:
                # If no running updates, get the latest completed one
                cur.execute("""
                    SELECT 
                        id, started_at, completed_at, total_sources,
                        sources_processed, status, message,
                        current_source, next_source, stage,
                        new_courts_found, courts_updated
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

        finally:
            cur.close()

    except Exception as e:
        logger.error(f"Error getting inventory status: {str(e)}")
        st.error("Error retrieving inventory status. Please try again later.")
        return None
    finally:
        if conn:
            conn.close()


# Add update button and handle update process
col1, col2 = st.columns([2, 1])
with col1:
    # Get available court types from database
    court_types = [f"{ct} Courts" for ct in get_court_types()] or ["All Courts"]
    update_type = st.selectbox(
        "Select Update Type",
        options=court_types,
        key="update_type_select"
    )

    def start_update_process(update_type):
        """Start the court inventory update process"""
        try:
            logger.info(f"Starting court inventory update for {update_type}")

            # Convert update type to expected format
            court_type = update_type.lower().split()[0]

            # Start the update process
            result = update_court_inventory(court_type=court_type)
            logger.info(f"Update process result: {result}")

            if result and isinstance(result, dict):
                if result.get('status') == 'completed':
                    st.success(f"Update completed: Found {result.get('new_courts', 0)} new courts, updated {result.get('updated_courts', 0)} existing courts")
                elif result.get('status') == 'error':
                    st.error(f"Error during update: {result.get('message', 'Unknown error occurred')}")
                    logger.error(f"Update process error: {result.get('message')}")
                else:
                    st.info("Update process started. Monitoring progress...")
                    logger.info("Update process initiated successfully")

                # Force refresh of status
                st.session_state.update_running = True
            else:
                error_msg = "Invalid response from update process"
                logger.error(error_msg)
                st.error(error_msg)

        except Exception as e:
            error_msg = f"Error updating inventory: {str(e)}"
            logger.error(error_msg, exc_info=True)
            st.error(error_msg)

    if st.button("Update Court Inventory Now", key="update_inventory_button"):
        start_update_process(update_type)

# Initialize session state for progress tracking
if 'update_running' not in st.session_state:
    st.session_state.update_running = False

# Update status section every second when running
if st.session_state.update_running:
    status = get_inventory_status()
    if status and status.get('status') == 'running':
        display_status_section()
    else:
        st.session_state.update_running = False
        display_status_section()
else:
    display_status_section()


# Display court statistics
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

    # Display detailed statistics
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

    # Add summary metrics before filters
    total_sources = len(source_df)
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Directory Sources", f"{total_sources:,}")
    with col2:
        active_sources = len(source_df[source_df['Status'] == 'Active'])
        st.metric("Active Sources", f"{active_sources:,}")
    with col3:
        total_courts = source_df['Courts Tracked'].sum()
        st.metric("Total Courts Tracked", f"{total_courts:,}")

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

    # Add filtered count
    if len(filtered_df) != len(source_df):
        st.caption(f"Showing {len(filtered_df):,} of {len(source_df):,} total sources")
else:
    st.info("No court sources configured")

# Add explanatory text about the court discovery process
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