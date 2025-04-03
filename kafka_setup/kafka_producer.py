from kafka import KafkaProducer
import json
import time

KAFKA_BROKER = 'localhost:9092'
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
    while time.time() < end_time:
        with open('data_ingestion/weather_data.json', 'r') as file:
            weather_data = json.load(file)
            
        producer.send(TOPIC, weather_data)
        print(f"Weather data sent to topic {TOPIC}")
        producer.flush()
        time.sleep(REQUEST_INTERVAL)
    producer.close()
    print("Producer closed.")
    
if __name__ == "__main__":
    send_weather_data()
    print("Weather data should be sent to Kafka topic :)")
    