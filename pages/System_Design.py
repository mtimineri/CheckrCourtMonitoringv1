import streamlit as st
import json

# Page configuration
st.set_page_config(
    page_title="System Design | Court Monitoring Platform",
    page_icon="⚖️",
    layout="wide"
)

st.title("System Design Documentation")
st.markdown("Technical documentation of the Court Monitoring Platform architecture and components")

# System Overview
st.header("System Overview")
st.markdown("""
The Court Monitoring Platform is a comprehensive system designed to track and visualize court information 
across the United States. The system consists of several key components working together to provide 
real-time court status information and analytics.
""")

# Core Components
st.subheader("Core Components")

col1, col2 = st.columns(2)

with col1:
    st.markdown("""
    #### Web Interface (Streamlit)
    - Interactive data visualization dashboard
    - Real-time court status monitoring
    - Geospatial court mapping
    - Advanced filtering and search capabilities

    #### Data Storage (PostgreSQL)
    - Court information database
    - Historical status tracking
    - Maintenance schedules
    - API usage logs
    """)

with col2:
    st.markdown("""
    #### Data Collection System
    - Automated web scrapers for court websites
    - Scheduled data updates
    - Intelligent content extraction
    - Status change detection

    #### AI Integration (OpenAI GPT-4o)
    - Intelligent content analysis
    - Status classification
    - Maintenance notice detection
    - Data extraction optimization
    """)

# Court Discovery Process
st.header("Enhanced Court Discovery Process")
st.markdown("""
### 1. Source Management
- Validated URL collection
- SSL verification handling
- Automatic URL cleaning and normalization
- Source reliability tracking

### 2. Content Extraction
- Robust HTML processing with Trafilatura
- Error recovery mechanisms
- Rate limiting compliance
- Content validation checks

### 3. AI-Powered Analysis
The system uses OpenAI's GPT-4o model (released May 2024) to:
- Extract structured court information
- Verify court authenticity
- Classify court types
- Determine operational status
- Extract contact information
""")

# Example Court Data Structure
st.subheader("Court Data Structure Example")
example_court = {
    "name": "Example District Court",
    "type": "District Courts",
    "status": "Open",
    "verified": True,
    "confidence": 0.95,
    "contact_info": {
        "phone": "(555) 123-4567",
        "email": "court@example.gov",
        "hours": "Mon-Fri 9:00 AM - 5:00 PM"
    }
}

# Display the example with syntax highlighting
st.code(json.dumps(example_court, indent=2), language="json")

# Real-time Progress Tracking
st.header("Real-time Progress Tracking")
st.markdown("""
### Progress Monitoring Features
1. **Source Processing**
   - Real-time progress indicators
   - Current source tracking
   - Processing stage display
   - Error reporting

2. **Status Updates**
   - Live progress percentage
   - Sources processed counter
   - New courts discovered
   - Courts updated tracking

3. **Error Handling**
   - Detailed error logging
   - SSL verification management
   - Invalid URL detection
   - Source validation failures
""")

# Database Schema
st.header("Database Architecture")
st.markdown("""
### Core Tables

1. **courts**
   - Court basic information
   - Location data
   - Current status
   - Maintenance schedules

2. **jurisdictions**
   - Hierarchical jurisdiction data
   - Parent-child relationships
   - Jurisdiction types

3. **court_sources**
   - Source URLs for scraping
   - Update frequencies
   - Last check timestamps
   - Source reliability metrics

4. **inventory_updates**
   - Scraping run logs
   - Progress tracking
   - Error reporting
   - Performance metrics
""")

# Future Enhancements
st.header("Future Enhancements")
st.markdown("""
### Planned Features
1. Historical status tracking
2. Advanced analytics dashboard
3. API endpoint for external access
4. Enhanced maintenance predictions
5. Mobile-optimized interface

### Technical Improvements
1. Enhanced concurrent scraping
2. Machine learning for status prediction
3. Automated source discovery
4. Real-time notification system
""")