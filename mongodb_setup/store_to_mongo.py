from kafka import KafkaConsumer
from pymongo import MongoClient, InsertOne, errors
from dotenv import load_dotenv
import json
import time
import os

load_dotenv(dotenv_path="config/.env")
KAFKA_BROKER = 'localhost:9092'
TOPIC = 'weather_data'
TIME_OUT = 600

# MONGO_URI = 'mongodb://localhost:27017/'
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = 'weather_db'
COLLECTION_NAME = 'weather_data'

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

"""
compound index, ascending order
unique = true 
ensures that the combination of StationId and ObservationTime is unique
"""

collection.create_index(
    [("StationId", 1), ("ObservationTime", 1)],
    unique = True
)

def create_consumer():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        api_version=(3, 9, 0),
        auto_offset_reset='earliest',
        value_deserializer = lambda x: json.loads(x.decode('utf-8')),
        enable_auto_commit=True
    )
    return consumer
def consume_weather_data(): 
    
    consumer = create_consumer()
    print("Starting to consume messages from Kafka & store them to MongoDB :)")
    last_message_time = time.time()
    
    while True:
        messages = consumer.poll(timeout_ms=1000)
        if not messages:
            if time.time() - last_message_time > TIME_OUT:
                print("No new messages received for a while. Exiting :)")
                break
        for topic_partition, records in messages.items():
            for record in records:
                last_message_time = time.time()
                data = record.value

                # print(type(data))
                if isinstance(data, dict):
                    filtered_data = {
                            "StationName": data.get('StationName'),
                            "StationId": data.get('StationId'),
                            "ObservationTime": data.get('ObsTime', {}).get('DateTime'),
                            "Weather": data.get('WeatherElement', {}).get('Weather'),
                            "AirTemperature": data.get('WeatherElement', {}).get('AirTemperature'),
                            "WindSpeed": data.get('WeatherElement', {}).get('WindSpeed')
                        }
                    
                    key = {
                        "StationId": filtered_data["StationId"],
                        "ObservationTime": filtered_data["ObservationTime"]
                    }
                    try:
                        collection.update_one(key, {"$setOnInsert": filtered_data}, upsert=True)
                        print(f"Inserted/Updated record into MongoDB: {filtered_data}")
                    except errors.DuplicateKeyError:
                        print(f"Duplicate entry found for {key}. Skipping insert.")
                else:
                    print("Received data is not in the expected format :(")

    consumer.close()
    print("Consumer closed.")
    client.close()
    print("MongoDB connection closed.")

def test():
    try:
        # Test the connection to MongoDB
        dbs = client.list_database_names()
        if dbs:
            print("MongoDB connection successful! :)")
        else:
            print("No databases found. MongoDB connection failed :(")
    except Exception as e:
        print(f"An error occurred: {e}") 
        
if __name__ == "__main__":
    test()
    consume_weather_data()
    