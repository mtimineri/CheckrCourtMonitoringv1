import streamlit as st

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

### Core Components

1. **Web Interface** (Streamlit)
   - Interactive data visualization dashboard
   - Real-time court status monitoring
   - Geospatial court mapping
   - Advanced filtering and search capabilities

2. **Data Collection System**
   - Automated web scrapers for court websites
   - Scheduled data updates
   - Intelligent content extraction
   - Status change detection

3. **Data Storage** (PostgreSQL)
   - Court information database
   - Historical status tracking
   - Maintenance schedules
   - API usage logs

4. **AI Integration** (OpenAI)
   - Intelligent content analysis
   - Status classification
   - Maintenance notice detection
   - Data extraction optimization
""")

# Technical Architecture
st.header("Technical Architecture")

# Data Collection
st.subheader("Data Collection System")
st.markdown("""
### Web Scraping Infrastructure

The platform uses a sophisticated web scraping system built with:
- **Trafilatura**: For efficient HTML content extraction
- **Python AsyncIO**: For concurrent scraping operations
- **Rate Limiting**: To respect court website resources

### Scraping Process
1. Court websites are accessed through configured source URLs
2. Raw HTML content is extracted and cleaned
3. Content is processed through OpenAI for structured data extraction
4. Extracted data is validated and stored in the database

### OpenAI Integration
The system uses GPT-4o (released May 13, 2024) for:
- Extracting court status information
- Identifying maintenance notices
- Parsing operational hours
- Detecting court service changes

Example of AI processing:
```python
{
    "name": "Example Court",
    "status": "Open",
    "maintenance_notice": "Scheduled system upgrade",
    "maintenance_start": "2024-03-01",
    "maintenance_end": "2024-03-02"
}
```
""")

# Database Schema
st.subheader("Database Architecture")
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

4. **inventory_updates**
   - Scraping run logs
   - Progress tracking
   - Error reporting

5. **api_usage**
   - OpenAI API call tracking
   - Token usage monitoring
   - Error logging
""")

# Technology Stack
st.header("Technology Stack")
st.markdown("""
### Frontend
- **Streamlit**: Web interface framework
- **Plotly**: Interactive visualizations
- **Pandas**: Data manipulation and analysis

### Backend
- **Python 3.11**: Core programming language
- **PostgreSQL**: Primary database
- **Trafilatura**: Web scraping
- **OpenAI API**: Content analysis

### Key Features
1. **Real-time Updates**
   - Automatic court status monitoring
   - Immediate status change detection
   - Live dashboard updates

2. **Data Visualization**
   - Interactive maps
   - Status distributions
   - Maintenance schedules
   - Jurisdiction hierarchies

3. **Search and Filtering**
   - Full-text search
   - Multi-criteria filtering
   - Jurisdiction-based filtering
   - Maintenance status filtering
""")

# Monitoring and Maintenance
st.header("Monitoring and Maintenance")
st.markdown("""
### System Monitoring
- Real-time scraping status tracking
- API usage monitoring
- Error logging and alerting
- Performance metrics collection

### Data Quality
- Automated validation checks
- AI-powered content verification
- Regular data audits
- Source reliability tracking

### Maintenance Procedures
1. Regular system health checks
2. Database optimization
3. API quota management
4. Source URL verification
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
