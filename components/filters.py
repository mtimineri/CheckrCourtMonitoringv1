import streamlit as st

def create_filters(court_types, court_statuses):
    with st.sidebar:
        st.markdown("## Filters")
        
        # Search box
        search_term = st.text_input("Search Courts", "")
        
        # Court type filter
        st.markdown("### Court Type")
        selected_types = st.multiselect(
            "Select court types",
            options=court_types,
            default=court_types
        )
        
        # Status filter
        st.markdown("### Status")
        selected_statuses = st.multiselect(
            "Select status",
            options=court_statuses,
            default=court_statuses
        )
        
        return search_term, selected_types, selected_statuses
