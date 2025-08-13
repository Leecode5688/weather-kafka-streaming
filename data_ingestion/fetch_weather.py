from dotenv import load_dotenv
import requests
import json
import time
import os

#Load API key
load_dotenv(dotenv_path="config/.env")
API_KEY = os.getenv("CWB_API_KEY")

API_URL = f"https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0001-001?Authorization={API_KEY}"
OUTPUT_FILE = "data_ingestion/weather_data.json"

station_ids_of_interest=["C0C730"]
FETCH_INTERVAL = 60
RUN_DURATION = 600

def test_api():
    """Send a request to the API, print the response status and data."""
    response = requests.get(API_URL)

    print(f"Status Code: {response.status_code}")
    if response.status_code == 200:
        print("API request successful!")
        try:
            json_data = response.json()
            #preety print the JSON data
            print(json.dumps(json_data, indent=4))
        except ValueError:
            print("Error decoding JSON response")
    else:
        print(f"API request failed with status code {response.status_code}")
        print("Response Content:")
        print(response.text)  

def fetch_weather_data():
    response = requests.get(API_URL)
    
    if response.status_code == 200:
        data = response.json()
        filter_and_save_data(data, station_ids_of_interest, output_file=OUTPUT_FILE)
    else:   
        print(f"Failed to fetch data: {response.status_code}")
        print("Response Content:")
        print(response.text)
"""
filter the result and construct the data
only keep the data of the stations that we are interested in
(for example,  "StationId": "C0C730" => "StationName": "中大臨海站")
save the result to a json file
"""

def filter_and_save_data(data, station_ids_of_interest, output_file):
    if not isinstance(data, dict):
        print("Error: Expected a dictionary")
        return
    existing_data = []
    if os.path.exists(output_file):
        with open(output_file, "r", encoding='utf-8') as file:
            try: 
                existing_data = json.load(file)
            except json.JSONDecodeError:
                print("Error decoding existing JSON data. Starting with an empty list.")
    
    unique_entries = {(entry["StationId"], entry["ObsTime"]["DateTime"]) for entry in existing_data}
    locations = data["records"]["Station"]
    new_data = []
    
    for entry in locations:
        if entry["StationId"] in station_ids_of_interest:
            unique_key = (entry["StationId"], entry["ObsTime"]["DateTime"])
            if unique_key not in unique_entries:
                unique_entries.add(unique_key)
                new_data.append(entry)
    if not new_data:
        print("No new data found for the specified station IDs.")
        return
    combined_data = existing_data + new_data
    with open(output_file, "w", encoding = 'utf-8') as file:
        #ensure ascii=False to keep the unicode characters (=> support Chinese)
        json.dump(combined_data, file, ensure_ascii=False, indent=4)
        print(f"Filtered data saved to {output_file}")
    
def run_periodically():
    start_time = time.time()
    end_time = start_time + RUN_DURATION

    while time.time() < end_time:
        fetch_weather_data()
        time.sleep(FETCH_INTERVAL)

if __name__ == "__main__":
    # test_api()
    run_periodically()
    print("Weather data should be fetched and saved")