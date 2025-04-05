from kafka import KafkaProducer
import json
import time

KAFKA_BROKER = 'localhost:9092'
"""
WARNING: Due to limitations in metric names, topics with a period ('.') or 
underscore ('_') could collide. To avoid issues it is best to use either, but
not both.
"""
TOPIC = "weather_data"
REQUEST_INTERVAL = 20
RUN_DURATION = 600
def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        api_version=(3, 9, 0),
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
def send_weather_data():
    producer = create_producer()
    start_time = time.time()
    end_time = start_time + RUN_DURATION
    
    #only send when data has changed
    # last_sent_unique_key = None
    sent_keys = set()
    
    while time.time() < end_time:
        try: 
            with open('data_ingestion/weather_data.json', 'r') as file:
                weather_data = json.load(file)
        except Exception as e:
            print(f"Error reading file: {e}")
            time.sleep(REQUEST_INTERVAL)
            continue
        if isinstance(weather_data, list) and weather_data:
            new_data_sent = False
            for entry in weather_data:
                key = (entry.get('StationId'), entry.get('ObsTime', {}).get("DateTime"))
                if key not in sent_keys:
                    sent_keys.add(key)
                    new_data_sent = True
                    producer.send(TOPIC, entry)
                    
            if new_data_sent:
                print(f"New weather data sent to topic {TOPIC} :)")
                producer.flush()
            else:
                print("No new data to send :(")
            time.sleep(REQUEST_INTERVAL)

    producer.close()
    print("Producer closed.")
    
if __name__ == "__main__":
    send_weather_data()
    print("Weather data should be sent to Kafka topic :)")
    