import pandas as pd

# Mock court data
court_data = {
    'name': [
        'Supreme Court of the United States',
        'US Court of Appeals for the Ninth Circuit',
        'New York Southern District Court',
        'California Central District Court',
        'Texas Northern District Court',
    ],
    'type': [
        'Supreme Court',
        'Appeals Court',
        'District Court',
        'District Court',
        'District Court',
    ],
    'status': [
        'Open',
        'Limited Operations',
        'Open',
        'Open',
        'Limited Operations',
    ],
    'lat': [
        38.8897,
        37.7786,
        40.7128,
        34.0522,
        32.7767,
    ],
    'lon': [
        -77.0089,
        -122.4192,
        -74.0060,
        -118.2437,
        -96.7970,
    ],
    'address': [
        '1 First Street, NE, Washington, DC 20543',
        '95 7th Street, San Francisco, CA 94103',
        '500 Pearl Street, New York, NY 10007',
        '350 W 1st Street, Los Angeles, CA 90012',
        '1100 Commerce Street, Dallas, TX 75242',
    ],
    'image_url': [
        'https://images.unsplash.com/photo-1564596489416-23196d12d85c',
        'https://images.unsplash.com/photo-1564595686486-c6e5cbdbe12c',
        'https://images.unsplash.com/photo-1600786288398-e795cfac80aa',
        'https://images.unsplash.com/photo-1521984692647-a41fed613ec7',
        'https://images.unsplash.com/photo-1685747750264-a4e932005dde',
    ]
}

def get_court_data():
    return pd.DataFrame(court_data)

def get_court_types():
    return sorted(list(set(court_data['type'])))

def get_court_statuses():
    return sorted(list(set(court_data['status'])))
