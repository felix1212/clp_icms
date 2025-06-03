import requests
import time
from datetime import datetime, timedelta, timezone
import json
import configparser
import os


def load_config():
    """Load configuration from settings.ini file."""
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'settings.ini')
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    config.read(config_path)
    return config


# Load configuration
config = load_config()

# === CONFIGURATION ===
USERNAME = config['Authentication']['username']
PASSWORD = config['Authentication']['password']
TOKEN_URL = config['Authentication']['token_url']
DATA_URL = config['API']['data_url']
TIMEZONE = timezone(timedelta(hours=int(config['Time']['timezone_hours'])))
WINDOW_MINUTES = int(config['Time']['window_minutes'])
QA = config['API']['qa']
STRIPMETA = config['API']['stripmeta']
LOOP_INTERVAL_SECONDS = int(config['Time']['loop_interval_seconds'])

def get_time_range(minutes: int):
    """Return (start_time_str, end_time_str) in ISO format for GMT+8."""
    end_time = datetime.now(TIMEZONE)
    start_time = end_time - timedelta(minutes=minutes)
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    return start_str, end_str


def build_query_url(base_url: str, qa: str, stripmeta: str, qc_list: list[str]) -> str:
    """Construct the full query URL with multiple qc parameters."""
    qc_query = "&".join([f"qc={qc}" for qc in qc_list])
    return f"{base_url}?qa={qa}&stripmeta={stripmeta}&{qc_query}"


def get_auth_token(username: str, password: str, token_url: str) -> str:
    """Get the access token using basic authentication."""
    auth = (username, password)
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(token_url, auth=auth, data={"grant_type": "client_credentials"}, headers=headers)
    response.raise_for_status()
    access_token = response.json().get("access_token")
    if not access_token:
        raise ValueError("Access token not found in response.")
    return access_token


def fetch_data(query_url: str, access_token: str) -> dict:
    """Call the data API and return the JSON response."""
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(query_url, headers=headers)
    response.raise_for_status()
    return response.json()


def main():
    # Step 1: Calculate time range
    start_time, end_time = get_time_range(WINDOW_MINUTES)
    print(f"Time window: {start_time} → {end_time}")

    # Step 2: Build query condition list
    qc_list = [
        f"DAYTIME,<=,{end_time}",
        f"DAYTIME,>,{start_time}"
    ]
    print("Query conditions:", qc_list)

    # Step 3: Get access token
    token = get_auth_token(USERNAME, PASSWORD, TOKEN_URL)
    print("Access token acquired.")

    # Step 4: Build full query URL
    query_url = build_query_url(DATA_URL, QA, STRIPMETA, qc_list)
    print("Query URL:", query_url)

    # Step 5: Fetch data
    result = fetch_data(query_url, token)
    print("Data retrieved:")
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print(f"[ERROR] {e}")
        print(f"Sleeping {LOOP_INTERVAL_SECONDS} seconds...\n")
        time.sleep(LOOP_INTERVAL_SECONDS)
