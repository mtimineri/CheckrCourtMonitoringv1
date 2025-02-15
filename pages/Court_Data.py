import streamlit as st
import pandas as pd
from court_data import get_court_data

# Page configuration
st.set_page_config(
    page_title="Court Data | Court Monitoring Platform",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.header("Court Data")
df = get_court_data()

if df.empty:
    st.warning("No court data available. Please run the scraper to collect data.")
else:
    # Add search filter
    search = st.text_input("Search courts", "")
    
    # Apply search filter
    filtered_df = df.copy()
    if search:
        search_mask = pd.Series(False, index=filtered_df.index)
        for col in ['name', 'address']:
            if col in filtered_df.columns:
                search_mask |= filtered_df[col].str.contains(search, case=False, na=False)
        filtered_df = filtered_df[search_mask]
    
    # Display the table with available columns
    if not filtered_df.empty:
        display_columns = [col for col in ['name', 'type', 'status', 'address'] 
                         if col in filtered_df.columns]
        st.dataframe(
            filtered_df[display_columns],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No courts match the search criteria.")
