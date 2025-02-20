mport requests
import json
import logging
import boto3
import base64
import time
from botocore.exceptions import ClientError

#Config logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# GitHub Configuration
GITHUB_REPO = "alexvera626/ADSB"
GITHUB_FILE_PATH = "adsb.geojson"  # Path inside the repo
GITHUB_BRANCH = "main"
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"

# aws Secrets Manager Configuration
SECRET_NAME = "GITHUB_TOKEN"  # Must exist in AWS Secrets Manager
REGION_NAME = "us-west-1"

# ADS-B API Configuration
BASE_URL = "https://opendata.adsb.fi/api/v2/icao/"
hex_codes = [
    "A1C5F2", "A1ECD6", "A2097B", "AD535A", "ADFE10", "ADFE12", "ADFE15", "ADFE17", "ADFE18", "ADFE1A",
    "ADFE1B", "ADFE1C", "AE109A", "AE10C1", "AE10E8", "AE10E9", "AE10EA", "AE10EB", "AE10EC", "AE20C1", 
    "AE2125", "AE2126", "AE2237", "AE2238", "AE223A", "AE223B", "AE223C", "AE223D", "AE223E", "AE223F", 
    "AE265B", "AE265C", "AE265D", "AE265E", "AE2660", "AE2661", "AE2662", "AE2663", "AE2664", "AE2665", 
    "AE2666", "AE2667", "AE2668", "AE266A", "AE266B", "AE266C", "AE266D", "AE266E", "AE266F", "AE2672", 
    "AE2673", "AE2674", "AE2675", "AE2676", "AE2677", "AE2678", "AE2679", "AE267A", "AE267B", "AE267C", 
    "AE267E", "AE267F", "AE2680", "AE2681", "AE2682", "AE2683", "AE2684", "AE2685", "AE2686", "AE2687", 
    "AE2688", "AE2689", "AE268A", "AE268B", "AE268C", "AE268D", "AE268E", "AE268F", "AE2690", "AE2691", 
    "AE2692", "AE2693", "AE2694", "AE2695", "AE2696", "AE2697", "AE2698", "AE2699", "AE269A", "AE269B", 
    "AE269C", "AE269D", "AE269E", "AE269F", "AE26A0", "AE26A1", "AE26A2", "AE26A3", "AE26A4", "AE26A5", 
    "AE26A6", "AE26A7", "AE26A8", "AE26A9", "AE26AA", "AE26AB", "AE26AC", "AE26AD", "AE26AE", "AE26AF", 
    "AE26B0", "AE26B1", "AE26B2", "AE26B3", "AE26B4", "AE26B5", "AE26B6", "AE26B7", "AE26B8", "AE26BA", 
    "AE26BB", "AE26BC", "AE26BD", "AE26BE", "AE2708", "AE2709", "AE270A", "AE272E", "AE2730", "AE27F2", 
    "AE27F3", "AE27F4", "AE27F5", "AE27F6", "AE27F7", "AE27F8", "AE27F9", "AE27FA", "AE27FB", "AE27FC", 
    "AE27FE", "AE27FF", "AE2900", "AE2901", "AE2902", "AE2903", "AE2905", "AE2906", "AE2907", "AE2908", 
    "AE2909", "AE290A", "AE290C", "AE290D", "AE290E", "AE290F", "AE2910", "AE2911", "AE2912", "AE2913", 
    "AE2914", "AE2915", "AE2916", "AE2917", "AE2918", "AE2919", "AE291A", "AE2BEF", "AE2BF0", "AE2BF1", 
    "AE2BF2", "AE4A4B", "AE4A4C", "AE4ADF", "AE4AE0", "AE4AE1", "AE4AE2", "AE4AE3", "AE4AE4", "AE4AE5", 
    "AE4CF7", "AE4DFE", "AE4DFF", "AE4E00", "AE4E01", "AE4E02", "AE4E03", "AE509F", "AE57D1", "AE57D2", 
    "AE57D3", "AE57D4", "AE57D5", "AE5DEB", "AE5DEC", "AE5DED", "AE5DEE", "AE5DEF", "AE6CE7", "AE6CE8", 
    "A32EAF", "A9079F", "AE6014", "A3238A", "A1DA3B", "A4CAAA", "AE4D35", "AE2761", "AE278E", "AE277E", 
    "AE743B", "AE1FCA", "AE1DAB", "A3524F", "AE4B62", "AE4F4D", "AC4597", "A5F058", "A5726D", "A385E3", 
    "A90F0D", "A145F7", "A14D65", "A154D3", "A1588A", "A0A6E5", "A23434", "A4D855", "A4DC0C", "A54ED2", 
    "A6DC21", "A1F894", "A054AD", "A0F2A9", "A117CF", "A13F4E", "A16926", "A9F008", "A44360", "AE2606",  
    "AE4F8A", "AE1841", "AE2778", "AE4D3F", "AE4FA1", "AE1843", "AE6BA4", "AE6988", "AE2793", "AE4FDA", 
    "AE4FCF", "A36968", "AD395A", "AD5711", "AED61D", "AD60D8", "AE279E", "AE743C", "AE4FF1", "AE4D5A",
    "A7FB70"
]
CHUNK_SIZE = 50  # Fetch data in batches of 50/ Total A/C Count 261

#Function to retrieve GitHub token from AWS Secrets Manager
def get_github_token():
    """Retrieves the GitHub token from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=REGION_NAME)
    try:
        response = client.get_secret_value(SecretId=SECRET_NAME)
        if "SecretString" in response:
            secret_data = json.loads(response["SecretString"])
            return secret_data.get("GITHUB_TOKEN")
    except ClientError as e:
        logging.error(f"❌ Error retrieving GitHub token: {e}")
        return None

#Fetch ADS-B Data
def fetch_adsb_data():
    """Fetches aircraft data from ADS-B API and converts it to GeoJSON."""
    geojson_output = {
        "type": "FeatureCollection",
        "features": [],
        "metadata": {"updated_at": time.strftime("%Y-%m-%d %H:%M:%S")}
    }

    for i in range(0, len(hex_codes), CHUNK_SIZE):
        hex_chunk = hex_codes[i:i + CHUNK_SIZE]
        url = f"{BASE_URL}{','.join(hex_chunk)}"

        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                geojson_output["features"].extend(convert_to_geojson(data)["features"])
            else:
                logging.error(f"❌ API Error {response.status_code}: {response.text}")
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ Request failed: {e}")

        time.sleep(1)  # rate limter

    return geojson_output

#Convert ADS-B Data to GeoJSON
def convert_to_geojson(data):
    """Converts aircraft JSON data to GeoJSON format."""
    features = []
    if not data or "ac" not in data:
        return {"type": "FeatureCollection", "features": []}

    for ac in data["ac"]:
        if "lon" in ac and "lat" in ac:
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [ac["lon"], ac["lat"]]},
                "properties": {
                    "hex": ac.get("hex", "N/A"),
                    "flight": ac.get("flight", "N/A"),
                    "operator": ac.get("ownOp", "Unknown"),
                    "altitude": ac.get("alt_baro", "N/A"),
                    "ground_speed": ac.get("gs", "N/A"),
                    "track": ac.get("track", "N/A"),
                    "squawk": ac.get("squawk", "N/A"),
                    "messages": ac.get("messages", "N/A")
                }
            })
    return {"type": "FeatureCollection", "features": features}

# fetch current file SHA from GitHub
def get_file_sha(headers):
    """Retrieves the SHA of the existing file to ensure proper update."""
    response = requests.get(GITHUB_API_URL, headers=headers)
    if response.status_code == 200:
        return response.json().get("sha", None)
    elif response.status_code == 404:
        logging.info("✅ File does not exist yet. Creating a new one.")
        return None  # File doesn't exist, so no SHA needed
    else:
        logging.error(f"❌ Error getting file SHA: {response.text}")
        return None

# upload data to GitHub
def upload_to_github(geojson_data):
    """Uploads or updates the adsb.geojson file in GitHub repository."""
    token = get_github_token()
    if not token:
        logging.error("❌ GitHub token not found.")
        return

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "ADSB-Data-Bot"
    }

    json_data = json.dumps(geojson_data, indent=4)
    file_sha = get_file_sha(headers)

    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    payload = {
        "message": f"🔄 Auto-update ADS-B GeoJSON [{timestamp}]",
        "content": base64.b64encode(json_data.encode()).decode(),
        "branch": GITHUB_BRANCH,
    }
    
    if file_sha:
        payload["sha"] = file_sha

    response = requests.put(GITHUB_API_URL, headers=headers, json=payload)

    logging.info(f"📬 GitHub API Response: {response.status_code} - {response.text}")

#AWS Lambda Handler
def lambda_handler(event, context):
    """AWS Lambda entry point."""
    geojson_output = fetch_adsb_data()
    upload_to_github(geojson_output)
    return {"statusCode": 200, "body": "Success"}

