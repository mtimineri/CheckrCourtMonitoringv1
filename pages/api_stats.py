import streamlit as st
import pandas as pd
from court_data import get_api_usage_stats
from datetime import datetime

def format_timestamp(ts):
    """Format timestamp for display"""
    if ts is None:
        return "N/A"
    return pd.to_datetime(ts).strftime("%Y-%m-%d %H:%M:%S")

# Page configuration
st.set_page_config(
    page_title="API Usage | Court Monitoring Platform",
    page_icon="⚖️",
    layout="wide"
)

st.header("OpenAI API Usage Statistics")
st.markdown("Track and monitor OpenAI API usage across the platform")

# Get API usage statistics
stats = get_api_usage_stats()

if stats['overall']:
    # Display overall metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total API Calls", stats['overall']['total_calls'])

    with col2:
        st.metric("Total Tokens Used", f"{stats['overall']['total_tokens']:,}")

    with col3:
        success_rate = (stats['overall']['successful_calls'] / stats['overall']['total_calls'] * 100 
                       if stats['overall']['total_calls'] else 0)
        st.metric("Success Rate", f"{success_rate:.1f}%")

    with col4:
        st.metric("Last Call", format_timestamp(stats['overall']['last_call_time']))

    # Display model-wise usage
    st.subheader("Usage by Model")
    if stats['by_model']:
        model_df = pd.DataFrame(stats['by_model'])
        st.dataframe(
            model_df,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No model usage data available")

    # Display recent calls
    st.subheader("Recent API Calls")
    if stats['recent']:
        recent_df = pd.DataFrame(stats['recent'])
        recent_df['timestamp'] = pd.to_datetime(recent_df['timestamp']).dt.strftime("%Y-%m-%d %H:%M:%S")
        st.dataframe(
            recent_df[['timestamp', 'endpoint', 'model', 'tokens_used', 'success', 'error_message']],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No recent API calls recorded")

else:
    st.info("No API usage data available yet. Data will appear here once the court scraper makes API calls.")

# Add refresh button
if st.button("Refresh Statistics"):
    st.rerun()