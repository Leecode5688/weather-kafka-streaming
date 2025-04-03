from kafka import KafkaConsumer
from pymongo import MongoClient
import json
import time

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'weather_data'
TIME_OUT = 600

MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'weather_db'
COLLECTION_NAME = 'weather_data'

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

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

                if isinstance(data, list):
                    filtered_entries = []
                    for entry in data:
                        filtered_data = {
                            "StationName": entry.get('StationName'),
                            "StationId": entry.get('StationId'),
                            "ObservationTime": entry.get('ObsTime', {}).get('DateTime'),
                            "Weather": entry.get('WeatherElement', {}).get('Weather'),
                            "AirTemperature": entry.get('WeatherElement', {}).get('AirTemperature'),
                            "WindSpeed": entry.get('WeatherElement', {}).get('WindSpeed')
                        }
                        filtered_entries.append(filtered_data)
                    if filtered_entries:
                        # Insert the filtered data into MongoDB
                        collection.insert_many(filtered_entries)
                        print(f"Inserted {len(filtered_entries)} record(s) into MongoDB :)")
                else:
                    print("Received data is not in the expected format :(")
    consumer.close()
    print("Consumer closed.")
    client.close()
    print("MongoDB connection closed.")
        
if __name__ == "__main__":
    consume_weather_data()
    