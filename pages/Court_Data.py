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

    # Create filter dict
    filters = {}
    if search:
        filters['search'] = search
    if selected_status != "All":
        filters['status'] = selected_status
    if selected_type != "All":
        filters['type'] = selected_type

# Get filtered data
df = get_filtered_court_data(filters)

if df.empty:
    st.warning("No courts match the selected filters.")
else:
    # Display stats
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Courts", len(df))
    with col2:
        st.metric("Court Types", len(df['type'].unique()))
    with col3:
        st.metric("Jurisdictions", len(df['jurisdiction_name'].unique()))

    # Create main display table
    st.dataframe(
        df[[
            'name', 'type', 'status', 'jurisdiction_name',
            'parent_jurisdiction', 'address'
        ]].rename(columns={
            'jurisdiction_name': 'Jurisdiction',
            'parent_jurisdiction': 'Parent Jurisdiction',
            'name': 'Court Name',
            'type': 'Court Type',
            'status': 'Status',
            'address': 'Address'
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

        # Create the map using Plotly
        fig = px.scatter_mapbox(
            courts_with_coords,
            lat='lat',
            lon='lon',
            color='status',
            color_discrete_map=status_colors,
            hover_data=['name', 'type', 'address'],
            zoom=3,
            title="Court Locations by Status",
            mapbox_style="open-street-map"  # Changed map style
        )

        # Update layout with custom marker styling
        fig.update_traces(
            marker=dict(size=12),  # Increase marker size
            selector=dict(mode='markers')
        )

        fig.update_layout(
            margin={"r":0,"t":30,"l":0,"b":0},
            height=600,
            legend_title="Court Status",
            showlegend=True
        )

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

# Add explanatory text
st.markdown("""
### About Court Data

This page provides comprehensive information about courts across different jurisdictions:

- **Search**: Use the search box to find courts by name or address
- **Filters**: Filter courts by status and type using the sidebar
- **Map View**: Courts with location data are displayed on the map, colored by their operational status
- **Download**: Export the filtered data as a CSV file

The data is regularly updated through our court monitoring system.
""")