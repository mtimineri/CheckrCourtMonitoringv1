# Technical Documentation

## Architecture Overview

### Frontend Components

#### Map Visualization (components/map.py)
- Uses Plotly for interactive map rendering
- Implements real-time status updates
- Handles court selection and information display

#### Court Information Display (components/court_info.py)
- Displays detailed court information
- Shows maintenance schedules
- Implements status indicators

#### Search and Filters (components/filters.py)
- Multi-criteria filtering system
- Real-time search functionality
- Advanced filter combinations

### Backend Services

#### Data Collection (court_scraper.py)
- Implements web scraping using Trafilatura
- Handles rate limiting and error recovery
- Processes raw HTML content

#### AI Processing
- Uses OpenAI GPT-4o for content extraction
- Implements intelligent status detection
- Processes maintenance notices

#### Database Operations (court_data.py)
- PostgreSQL database integration
- Court information storage
- Maintenance schedule tracking

## Database Schema

### Courts Table
```sql
CREATE TABLE courts (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    lat FLOAT NOT NULL,
    lon FLOAT NOT NULL,
    address TEXT NOT NULL,
    image_url TEXT NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    maintenance_notice TEXT,
    maintenance_start TIMESTAMP,
    maintenance_end TIMESTAMP
);
```

### Scraper Status Table
```sql
CREATE TABLE scraper_status (
    id SERIAL PRIMARY KEY,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    courts_processed INTEGER DEFAULT 0,
    total_courts INTEGER,
    status VARCHAR(50) DEFAULT 'running',
    message TEXT,
    current_court TEXT,
    next_court TEXT,
    stage TEXT
);
```

## AI Integration

### OpenAI Configuration
- Model: GPT-4o (Latest version, released May 13, 2024)
- Content Type: JSON structured output
- Error Handling: Implements exponential backoff

### Data Processing Pattern
```python
system_prompt = """Extract court information from the provided text:
- name: {court_info['name']}
- type: {court_info['type']}
- status: one of [Open, Closed, Limited Operations]
- maintenance_notice: upcoming maintenance
- maintenance_start: YYYY-MM-DD format
- maintenance_end: YYYY-MM-DD format"""
```

## Development Guidelines

### Code Structure
- Follow PEP 8 style guide
- Implement proper error handling
- Use type hints for better code clarity
- Document all functions and classes

### Database Operations
- Use connection pooling
- Implement proper transaction handling
- Always close connections in finally blocks

### Error Handling
- Log all errors with appropriate levels
- Implement graceful degradation
- Maintain operation continuity

### Testing
- Unit tests for core functionality
- Integration tests for API endpoints
- End-to-end testing for critical paths

## Deployment

### Environment Setup
1. Configure PostgreSQL database
2. Set up environment variables
3. Install system dependencies
4. Configure Streamlit server

### Monitoring
- Track API usage
- Monitor scraper status
- Log system health metrics
- Track database performance

## Maintenance

### Regular Tasks
- Database optimization
- API quota management
- Source URL verification
- Content extraction validation

### Updates
- Regular dependency updates
- Security patches
- Performance optimizations
- Feature enhancements
