hex_codes = [
    'ADFE10', 'ADFE12', 'ADFE15', 'ADFE17', 'ADFE18', 'ADFE1A', 'ADFE1B', 'ADFE1C', 'AE109A',
    'AE10C1', 'AE10E8', 'AE10E9', 'AE10EA', 'AE10EB', 'AE10EC', 'AE20C1', 'AE2125', 'AE2126',
    'AE2237', 'AE2238', 'AE223A', 'AE223B', 'AE223C', 'AE223D', 'AE223E', 'AE223F', 'AE265B',
    'AE265C', 'AE265D', 'AE265E', 'AE2660', 'AE2661', 'AE2662', 'AE2663', 'AE2664', 'AE2665',
    'AE2666', 'AE2667', 'AE2668', 'AE266A', 'AE266B', 'AE266C', 'AE266D', 'AE266E', 'AE266F',
    'AE2672', 'AE2673', 'AE2674', 'AE2675', 'AE2676', 'AE2677', 'AE2678', 'AE2679', 'AE267A',
    'AE267B', 'AE267C', 'AE267E', 'AE267F', 'AE2680', 'AE2681', 'AE2682', 'AE2683', 'AE2684',
    'AE2685', 'AE2686', 'AE2687', 'AE2688', 'AE2689', 'AE268A', 'AE268B', 'AE268C', 'AE268D',
    'AE268E', 'AE268F', 'AE2690', 'AE2691', 'AE2692', 'AE2693', 'AE2694', 'AE2695', 'AE2696',
    'AE2697', 'AE2698', 'AE2699', 'AE269A', 'AE269B', 'AE269C', 'AE269D', 'AE269E', 'AE269F',
    'AE26A0', 'AE26A1', 'AE26A2', 'AE26A3', 'AE26A4', 'AE26A5', 'AE26A6', 'AE26A7', 'AE26A8',
    'AE26A9', 'AE26AA', 'AE26AB', 'AE26AC', 'AE26AD', 'AE26AE', 'AE26AF', 'AE26B0', 'AE26B1',
    'AE26B2', 'AE26B3', 'AE26B4', 'AE26B5', 'AE26B6', 'AE26B7', 'AE26B8', 'AE26BA', 'AE26BB',
    'A2097B', 'AD535A', 'A1C5F2', 'A1ECD6'
]

# API base URL
BASE_URL = "https://opendata.adsb.fi/api/v2/icao/"

# Function to fetch data with retries
def fetch_data(hex_codes_batch):
    """Fetches aircraft data from API, handling errors and retries."""
    url = f"{BASE_URL}{','.join(hex_codes_batch)}"
    attempts = 3  # Retry up to 3 times
    for attempt in range(attempts):
        try:
            response = requests.get(url, timeout=10)  # Add timeout
            if response.status_code == 200:
                return response.json()
            elif response.status_code in [429, 500]:  # Rate limited or server error
                logging.warning(f"Attempt {attempt + 1}: API error {response.status_code}. Retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logging.error(f"Error {response.status_code}: {response.text}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            time.sleep(2 ** attempt)
    return None

# JSON data to GeoJSON
//def convert_to_geojson(data):
    """Converts aircraft JSON data to GeoJSON format."""
    features = []
    if not data or 'ac' not in data:
        return {"type": "FeatureCollection", "features": []}

    for ac in data['ac']:
        if 'lon' in ac and 'lat' in ac:
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [ac['lon'], ac['lat']]
                },
                "properties": {
                    "hex": ac.get('hex', 'N/A'),
                    "flight": ac.get('flight', 'N/A'),
                    "desc": ac.get('desc', 'N/A'),
                    "operator": ac.get('ownOp', 'Unknown'),
                    "altitude": ac.get('alt_baro', 'N/A'),
                    "ground_speed": ac.get('gs', 'N/A'),
                    "track": ac.get('track', 'N/A'),
                    "squawk": ac.get('squawk', 'N/A'),
                    "year": ac.get('year', 'N/A'),
                    "category": ac.get('category', 'N/A'),
                    "rssi": ac.get('rssi', 'N/A'),
                    "messages": ac.get('messages', 'N/A')
                }
            }
            features.append(feature)

    return {"type": "FeatureCollection", "features": features}

# Splitting hex codes into chunks of 50 (not sure if needed :( senior look at this
CHUNK_SIZE = 50
geojson_output = {"type": "FeatureCollection", "features": []}

for i in range(0, len(hex_codes), CHUNK_SIZE):
    hex_chunk = hex_codes[i:i + CHUNK_SIZE]
    data = fetch_data(hex_chunk)
    
    if data:
        geojson_chunk = convert_to_geojson(data)
        geojson_output["features"].extend(geojson_chunk["features"])
    
    time.sleep(1)  # Respect rate limit (not sure if needed, only making one batch api call 

# Save GeoJSON output
with open('adsb.geojson', 'w') as geojson_file:
    json.dump(geojson_output, geojson_file, indent=4)

logging.info("GeoJSON file saved as 'adsb.geojson'")
