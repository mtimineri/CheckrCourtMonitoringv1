import streamlit as st
import pandas as pd
import plotly.express as px
from court_data import get_filtered_court_data, get_court_types, get_court_statuses
from datetime import datetime

# Page configuration
st.set_page_config(
    page_title="Court Data | Court Monitoring Platform",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Court Data")
st.markdown("Explore and filter court information across jurisdictions")

# Add explanatory text at the top
st.markdown("""
### About Court Data

This page provides comprehensive information about courts across different jurisdictions:

- **Search**: Use the search box to find courts by name or address
- **Filters**: Filter courts by status and type using the sidebar
- **Map View**: Courts with location data are displayed on the map, colored by their operational status
- **Download**: Export the filtered data as a CSV file
- **Maintenance**: View upcoming maintenance and planned downtimes

The data is regularly updated through our court monitoring system.
""")

# Initialize filters
with st.sidebar:
    st.header("Filters")

    # Text search
    search = st.text_input("Search courts", "", 
                          placeholder="Search by name or address")

    # Status filter
    status_options = ["All"] + get_court_statuses()
    selected_status = st.selectbox("Status", status_options)

    # Court type filter
    type_options = ["All"] + get_court_types()
    selected_type = st.selectbox("Court Type", type_options)

    # Maintenance filter
    show_maintenance = st.checkbox("Show courts with scheduled maintenance", value=False)

    # Create filter dict
    filters = {}
    if search:
        filters['search'] = search
    if selected_status != "All":
        filters['status'] = selected_status
    if selected_type != "All":
        filters['type'] = selected_type
    if show_maintenance:
        filters['has_maintenance'] = True

# Get filtered data
df = get_filtered_court_data(filters)

if df.empty:
    st.warning("No courts match the selected filters.")
else:
    # Display stats
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Courts", len(df))
    with col2:
        st.metric("Court Types", len(df['type'].unique()))
    with col3:
        st.metric("Jurisdictions", len(df['jurisdiction_name'].unique()))
    with col4:
        maintenance_count = len(df[df['maintenance_notice'].notna()])
        st.metric("Courts with Maintenance", maintenance_count)

    # Create main display table
    display_columns = [
        'name', 'type', 'status', 'jurisdiction_name',
        'parent_jurisdiction', 'address'
    ]

    # Add maintenance columns if there are any courts with maintenance
    if maintenance_count > 0:
        display_columns.extend(['maintenance_notice', 'maintenance_start', 'maintenance_end'])

    # Format maintenance dates
    if 'maintenance_start' in df.columns:
        df['maintenance_start'] = pd.to_datetime(df['maintenance_start']).dt.strftime('%Y-%m-%d')
    if 'maintenance_end' in df.columns:
        df['maintenance_end'] = pd.to_datetime(df['maintenance_end']).dt.strftime('%Y-%m-%d')

    st.dataframe(
        df[display_columns].rename(columns={
            'jurisdiction_name': 'Jurisdiction',
            'parent_jurisdiction': 'Parent Jurisdiction',
            'name': 'Court Name',
            'type': 'Court Type',
            'status': 'Status',
            'address': 'Address',
            'maintenance_notice': 'Maintenance Notice',
            'maintenance_start': 'Maintenance Start',
            'maintenance_end': 'Maintenance End'
        }),
        use_container_width=True,
        hide_index=True
    )

    # Display map if coordinates are available
    courts_with_coords = df.dropna(subset=['lat', 'lon'])
    if not courts_with_coords.empty:
        st.subheader("Court Locations")

        # Define status colors
        status_colors = {
            'Open': '#28a745',  # Green
            'Closed': '#dc3545',  # Red
            'Limited Operations': '#ffc107'  # Yellow
        }

        # Ensure status colors are applied correctly
        fig = px.scatter_mapbox(
            courts_with_coords,
            lat='lat',
            lon='lon',
            color='status',
            color_discrete_map=status_colors,
            hover_data=['name', 'type', 'address'],
            zoom=3,
            title="Court Locations by Status",
            mapbox_style="open-street-map"
        )

        # Update layout with custom marker styling
        fig.update_traces(
            marker=dict(size=12),
            selector=dict(mode='markers')
        )

        fig.update_layout(
            margin={"r":0,"t":30,"l":0,"b":0},
            height=600,
            legend_title="Court Status",
            showlegend=True
        )

        # Force color scheme
        for trace in fig.data:
            status = trace.name
            if status in status_colors:
                trace.marker.color = status_colors[status]

        st.plotly_chart(fig, use_container_width=True)

        # Add color legend explanation
        st.markdown("""
        **Status Colors:**
        - 🟢 Open: Normal operations
        - 🔴 Closed: Not currently operating
        - 🟡 Limited Operations: Partially operating or restricted services
        """)

    # Download option
    st.download_button(
        "Download Data as CSV",
        df.to_csv(index=False).encode('utf-8'),
        "court_data.csv",
        "text/csv",
        key='download-csv'
    )