from data_ingestion.fetch_weather import fetch_weather_data
from kafka_setup.kafka_producer import send_weather_data
from mongodb_setup.store_to_mongo import consume_weather_data as store_to_mongo
import threading
import time
import os

WEATHER_DATA_PATH = "data_ingestion/weather_data.json"
TIME_OUT = 60


def wait_for_file_ready(file_path):
    print(f"Waiting for {file_path} to be ready ;)")
    start_time = time.time()
    while time.time()-start_time < TIME_OUT:
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            print(f"{file_path} is ready :)")
            return True
        time.sleep(1)
    print(f"{file_path} is not ready within the timeout period :(")
    return False

def run_pipeline():
    print("Starting the data pipeline :)")
    fetch_weather_data()
    if not wait_for_file_ready(WEATHER_DATA_PATH):
        print("Weather data not ready. Exiting pipeline.")
        return

    print("Weather data fetched. Sending to Kafka :)")
    send_thread = threading.Thread(target=send_weather_data)
    print("Consume & store weather data to MongoDB :)")
    consume_and_store_thread = threading.Thread(target=store_to_mongo)
    
    send_thread.start()
    consume_and_store_thread.start()
    
    send_thread.join()
    consume_and_store_thread.join()
    print("Data pipeline completed :)")
    
if __name__ == "__main__":
    run_pipeline()
    print("Weather data pipeline should be running :)")
    
    
