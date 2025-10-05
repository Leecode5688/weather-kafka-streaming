from dotenv import load_dotenv
from config.config import API_URL, STATION_IDS_OF_INTEREST
import requests
import logging
import json
import os

station_ids_of_interest=STATION_IDS_OF_INTEREST

logger = logging.getLogger("fetcher_service.fetch_weather")

def fetch_weather_data():
    """
    fetch raw data from cwb taiwan's api, return the json response
    raise exception if the response is not successful
    """
    response = requests.get(API_URL)
    response.raise_for_status()
    return response.json()


def filter_and_save_data(raw_data, station_ids_of_interest):
    """
    filter the result and construct the data
    only keep the data of the stations that we are interested in
    (for example,  "StationId": "C0C730" => "StationName": "中大臨海站")

    Args:
        raw_data (dict): The raw JSON data from the API
        station_ids_of_interest (list): A list of station IDs to filter the data.

    Returns:
        list[dict]: Filtered list of station data
    
    Raises: 
        ValueError: if raw_data format is invalid
        
    """
    if not isinstance(raw_data, dict) or 'records' not in raw_data:
        raise ValueError("Invalid data format received from API")

    locations = raw_data.get("records", {}).get("Station", [])
    filtered = [
        entry for entry in locations
        if entry.get("StationId") in station_ids_of_interest
    ]
    if not filtered:
        logging.info("No data found for the specified station IDs of interest")
        return []

    return filtered

def get_weather():
    try:
        raw_data = fetch_weather_data()
        filtered_data = filter_and_save_data(raw_data, STATION_IDS_OF_INTEREST)
        return filtered_data
    except requests.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return []
    
