# Court Monitoring Platform

A comprehensive platform for tracking and visualizing court information across the United States, with advanced data collection and analysis capabilities.

## Features

- 🗺️ **Interactive Court Map**: Real-time visualization of court locations and operational status
- 📊 **Data Analytics**: Comprehensive court data analysis and filtering
- 🔍 **Advanced Search**: Multi-criteria search across jurisdictions
- 📱 **Responsive Design**: Optimized for both desktop and mobile viewing
- 🤖 **AI-Powered**: Intelligent content extraction using OpenAI's GPT-4o
- 📅 **Maintenance Tracking**: Monitor scheduled maintenance and planned downtimes

## Technology Stack

- **Frontend**: Streamlit
- **Backend**: Python 3.11
- **Database**: PostgreSQL
- **AI Integration**: OpenAI GPT-4o
- **Data Processing**: Pandas, Plotly
- **Web Scraping**: Trafilatura

## Getting Started

### Prerequisites

- Python 3.11+
- PostgreSQL database
- OpenAI API key

### Environment Variables

Set up the following environment variables:
```bash
DATABASE_URL=postgresql://user:password@host:port/database
OPENAI_API_KEY=your_openai_api_key
```

### Installation

1. Clone the repository:
```bash
git clone https://github.com/mtimineri/CheckrCourtMonitoringv1.git
cd CheckrCourtMonitoringv1
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Start the application:
```bash
streamlit run Court_Map.py
```

## Project Structure

```
├── components/              # Reusable UI components
│   ├── court_info.py       # Court information display
│   ├── map.py             # Map visualization
│   └── filters.py         # Search and filter components
├── pages/                  # Streamlit pages
│   ├── Court_Data.py      # Court data exploration
│   ├── Court_Hierarchy.py # Jurisdiction hierarchy
│   ├── Data_Scraper.py   # Scraper control
│   └── System_Design.py   # Documentation
├── court_scraper.py       # Web scraping logic
├── court_data.py          # Database operations
└── main.py                # Application entry point
```

## Features

### Court Map
- Interactive map showing all court locations
- Color-coded status indicators
- Click-to-view detailed information
- Real-time status updates

### Court Data
- Comprehensive court information display
- Advanced filtering capabilities
- Maintenance schedule tracking
- Data export functionality

### Hierarchy View
- Visual representation of court jurisdictions
- Interactive hierarchy navigation
- Detailed court system explanation

### Data Collection
- Automated web scraping
- AI-powered content extraction
- Status change detection
- Maintenance notice tracking

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
