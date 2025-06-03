import requests
import time
import sys
from datetime import datetime, timedelta, timezone
import json
import configparser
import os
from pathlib import Path
import logging

# Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

def load_config():
    """Load configuration from settings.ini file."""
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'settings.ini')
    
    if not os.path.exists(config_path):
        logger.error(f"Configuration file not found: {config_path}")
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    config.read(config_path)
    logger.debug("Configuration loaded successfully")
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
DD_API_KEY = config['Authentication']['dd_api_key']
DD_APPLICATION_KEY = config['Authentication']['dd_application_key']
DATADOG_URL = config['API']['datadog_url']
DD_HOST = config['API']['dd_host']
DD_SERVICE = config['API']['dd_service']
WRITE_TO_FILE = config['Config']['write_to_file'].lower() == 'true'

def get_time_range(minutes: int):
    """Return (start_time_str, end_time_str) in ISO format for GMT+8."""
    end_time = datetime.now(TIMEZONE)
    start_time = end_time - timedelta(minutes=minutes)
    end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S")
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    logger.debug(f"Time range calculated: {start_str} to {end_str}")
    return start_str, end_str

def build_query_url(base_url: str, qa: str, stripmeta: str, qc_list: list[str]) -> str:
    """Construct the full query URL with multiple qc parameters."""
    qc_query = "&".join([f"qc={qc}" for qc in qc_list])
    url = f"{base_url}?qa={qa}&stripmeta={stripmeta}&{qc_query}"
    logger.debug(f"Query URL built: {url}")
    return url

def get_auth_token(username: str, password: str, token_url: str) -> str:
    """Get the access token using basic authentication."""
    auth = (username, password)
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    logger.debug("Attempting to get authentication token")
    response = requests.post(token_url, auth=auth, data={"grant_type": "client_credentials"}, headers=headers)
    response.raise_for_status()
    access_token = response.json().get("access_token")
    if not access_token:
        logger.error("Access token not found in response")
        raise ValueError("Access token not found in response.")
    logger.debug("Authentication token acquired successfully")
    return access_token

def fetch_data(query_url: str, access_token: str) -> dict:
    """Call the data API and return the JSON response."""
    headers = {"Authorization": f"Bearer {access_token}"}
    logger.debug("Fetching data from API")
    response = requests.get(query_url, headers=headers)
    response.raise_for_status()
    logger.debug("Data fetched successfully")
    return response.json()

def transform_response(response_data: dict) -> list:
    """
    Transform the API response into the desired format.
    
    Args:
        response_data: The raw response from the API
        
    Returns:
        A list of transformed records
    """
    logger.debug("Starting response transformation")
    transformed_records = []
    
    # Extract the records from the nested structure
    records = response_data.get('objects', {}).get('ZXC_DATADOG_JOURNAL', [])
    logger.debug(f"Found {len(records)} records to transform")
    
    for record in records:
        attrs = record.get('attributes', {})
        
        # Extract and transform the fields
        transformed_record = {
            'DATADOG_JOURNAL_ID': attrs.get('DATADOG_JOURNAL_ID', {}).get('datavalue'),
            'DOC_NUMBER': attrs.get('DOC_NUMBER', {}).get('datavalue', '').replace('DOC:', ''),
            'MESSAGE_ID': attrs.get('MESSAGE_ID', {}).get('datavalue'),
            'MESSAGE_TYPE': attrs.get('MESSAGE_TYPE', {}).get('datavalue'),
            'MESSAGE_DIRECTION': attrs.get('MESSAGE_DIRECTION', {}).get('datavalue'),
            'RESPONSE_TIME': attrs.get('RESPONSE_TIME', {}).get('datavalue'),
            'STATUS': attrs.get('STATUS', {}).get('datavalue'),
            'REJECT_REASON': attrs.get('REJECT_REASON', {}).get('datavalue'),
            'HOST_NAME': attrs.get('HOST_NAME', {}).get('datavalue'),
            'PUBLIC_HOST_NAME': attrs.get('PUBLIC_HOST_NAME', {}).get('datavalue'),
            'HOST_IP_ADDRESS': attrs.get('HOST_IP_ADDRESS', {}).get('datavalue'),
            'PUBLIC_HOST_IP_ADDRESS': attrs.get('PUBLIC_HOST_IP_ADDRESS', {}).get('datavalue'),
            'hostname': DD_HOST,
            'service': DD_SERVICE
        }
        
        # Convert timestamps to epoch time
        daytime = attrs.get('DAYTIME', {}).get('datavalue')
        start_time = attrs.get('START_TIMESTAMPS', {}).get('datavalue')
        end_time = attrs.get('END_TIMESTAMPS', {}).get('datavalue')
        
        if daytime:
            daytime_dt = datetime.strptime(daytime, "%Y-%m-%dT%H:%M:%S")
            transformed_record['DAYTIME'] = int(daytime_dt.timestamp())
        
        if start_time:
            start_dt = datetime.strptime(start_time, "%Y-%m-%dT%H:%M:%S")
            transformed_record['START_TIMESTAMPS'] = int(start_dt.timestamp())
        
        if end_time:
            end_dt = datetime.strptime(end_time, "%Y-%m-%dT%H:%M:%S")
            transformed_record['END_TIMESTAMPS'] = int(end_dt.timestamp())
        
        transformed_records.append(transformed_record)
    
    logger.debug(f"Successfully transformed {len(transformed_records)} records")
    return transformed_records

def send_to_datadog(transformed_data: list) -> int:
    """
    Send transformed data to Datadog API.
    
    Args:
        transformed_data: List of transformed records to send
        
    Returns:
        HTTP status code from the API response
    """
    headers = {
        "DD-API-KEY": DD_API_KEY,
        "DD-APPLICATION-KEY": DD_APPLICATION_KEY,
        "Accept": "*/*",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip, deflate, br"
    }
    
    logger.debug("Sending data to Datadog API")
    try:
        response = requests.post(DATADOG_URL, headers=headers, json=transformed_data)
        response.raise_for_status()
        logger.info(f"Successfully sent {len(transformed_data)} records to Datadog")
        return response.status_code
    except requests.exceptions.RequestException as e:
        logger.error(f"Error sending data to Datadog: {e}")
        return getattr(e.response, 'status_code', 500) if hasattr(e, 'response') else 500

def main():
    # Step 1: Calculate time range
    start_time, end_time = get_time_range(WINDOW_MINUTES)
    logger.info(f"Time window: {start_time} → {end_time}")

    # Step 2: Build query condition list
    qc_list = [
        f"DAYTIME,<=,{end_time}",
        f"DAYTIME,>,{start_time}"
    ]
    logger.debug(f"Query conditions: {qc_list}")

    # Step 3: Get access token
    token = get_auth_token(USERNAME, PASSWORD, TOKEN_URL)
    logger.info("Access token acquired")

    # Step 4: Build full query URL
    query_url = build_query_url(DATA_URL, QA, STRIPMETA, qc_list)
    logger.debug(f"Query URL: {query_url}")

    # Step 5: Fetch data
    result = fetch_data(query_url, token)
    logger.debug("Raw data retrieved")
    logger.debug(json.dumps(result, indent=2, ensure_ascii=False))

    if WRITE_TO_FILE:
        timestamp = datetime.now(TIMEZONE).strftime("%Y%m%d_%H%M%S")
        raw_file = Path(f"raw_{timestamp}.json")
        with raw_file.open("w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        logger.info(f"Raw data written to {raw_file}")
    
    # Step 6: Transform the response
    transformed_data = transform_response(result)
    logger.debug("Transformed data:")
    logger.debug(json.dumps(transformed_data, indent=2, ensure_ascii=False))

    if WRITE_TO_FILE:
        transformed_file = Path(f"transformed_{timestamp}.json")
        with transformed_file.open("w", encoding="utf-8") as f:
            json.dump(transformed_data, f, indent=2, ensure_ascii=False)
        logger.info(f"Transformed data written to {transformed_file}")

    # Step 7: Send to Datadog
    status_code = send_to_datadog(transformed_data)
    logger.info(f"Datadog API response status code: {status_code}")


if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
        logger.info(f"Sleeping {LOOP_INTERVAL_SECONDS} seconds...\n")
        time.sleep(LOOP_INTERVAL_SECONDS)
