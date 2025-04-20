from kafka import KafkaConsumer
import json
import time

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'weather_data'
TIME_OUT = 600  
def create_consumer():
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        api_version=(3, 9),
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
    )
    
def consume_weather_data():
    consumer = create_consumer()
    print("Starting to consume messages from Kafka :)")
    last_message_time = time.time()
    sent_keys = set()  

    try:
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

                    if isinstance(data, dict):  
                        filtered_data = {
                            "StationName": data.get('StationName'),
                            "StationId": data.get('StationId'),
                            "ObservationTime": data.get('ObsTime', {}).get('DateTime'),
                            "Weather": data.get('WeatherElement', {}).get('Weather'),
                            "AirTemperature": data.get('WeatherElement', {}).get('AirTemperature'),
                            "WindSpeed": data.get('WeatherElement', {}).get('WindSpeed')
                        }

                        # Create a unique key based on StationId and ObservationTime
                        key = (filtered_data["StationId"], filtered_data["ObservationTime"])

                        if key not in sent_keys:
                            sent_keys.add(key)  
                            print("Filtered Data:")
                            print(json.dumps(filtered_data, indent=4, ensure_ascii=False))
                        else:
                            print(f"Duplicate data detected for StationId: {filtered_data['StationId']} at ObservationTime: {filtered_data['ObservationTime']}")
                    else:
                        print("Received data is not in the expected format. Expected a dictionary.")
                        
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        consumer.close()
        print("Consumer closed :)")

if __name__ == "__main__":
    consume_weather_data()
    print("Weather data should be consumed from Kafka topic :)")