import streamlit as st
import pandas as pd
from court_data import get_court_data, get_court_types, get_court_statuses
from components.map import create_court_map
from components.filters import create_filters
from components.court_info import display_court_info, display_status_legend

# Page configuration
st.set_page_config(
    page_title="Court Monitoring Platform",
    page_icon="⚖️",
    layout="wide"
)

# Load custom CSS
with open('styles.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Initialize session state
if 'selected_court' not in st.session_state:
    st.session_state.selected_court = None

# Header with Checkr logo
st.markdown("""
    <div class='header'>
        <div class='logo'>Checkr</div>
        <h1>Court Monitoring Platform</h1>
        <p>Real-time status monitoring of United States courts</p>
    </div>
""", unsafe_allow_html=True)

# Load data
df = get_court_data()
court_types = get_court_types()
court_statuses = get_court_statuses()

# Create filters
search_term, selected_types, selected_statuses = create_filters(court_types, court_statuses)

# Filter data
filtered_df = df[
    (df['type'].isin(selected_types)) &
    (df['status'].isin(selected_statuses))
]

if search_term:
    filtered_df = filtered_df[
        filtered_df['name'].str.contains(search_term, case=False) |
        filtered_df['address'].str.contains(search_term, case=False)
    ]

# Create main layout
col1, col2 = st.columns([7, 3])

with col1:
    # Display map
    st.markdown("<div class='map-container'>", unsafe_allow_html=True)
    fig = create_court_map(filtered_df, st.session_state.selected_court)

    # Handle map click events
    clicked_data = st.plotly_chart(fig, use_container_width=True, return_value=True)
    if clicked_data:
        try:
            clicked_court = clicked_data['points'][0]['text']
            if clicked_court != st.session_state.selected_court:
                st.session_state.selected_court = clicked_court
                st.experimental_rerun()
        except (KeyError, IndexError, TypeError):
            pass

    st.markdown("</div>", unsafe_allow_html=True)

with col2:
    # Display status legend
    display_status_legend()

    # Display court information
    if st.session_state.selected_court:
        court_info = df[df['name'] == st.session_state.selected_court].iloc[0].to_dict()
        display_court_info(court_info)