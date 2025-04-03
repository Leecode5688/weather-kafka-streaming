from kafka import KafkaConsumer
import json
import time

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'weather_data'
TIME_OUT = 600
def create_consumer():
    """
    'earliest' => Start reading messages from the beginning of the topic
    'latest' => Start reading messages from the end of the topic (which means only new messages).
    """
    consumer = KafkaConsumer(
        TOPIC, 
        bootstrap_servers=KAFKA_BROKER,
        api_version=(3, 9, 0),
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
    )
    return consumer
def consume_test():
    consumer = create_consumer()
    print("Starting to consume messages from Kafka :)")
    try: 
        for message in consumer:
            data = message.value
            print(f"Received data: {data}")
            print(f"Data consumed from Kafka: {data}")
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()  # Ensure the consumer is properly closed
        print("Consumer closed.")
        
def consume_weather_data():
    consumer = create_consumer()
    print("Starting to consume messages from Kafka :)")
    # timeout = 10
    last_message_time = time.time() 
    while True:
        messages = consumer.poll(timeout_ms = 1000)
        if not messages:
            if time.time() - last_message_time > TIME_OUT:
                print("No new messages received for a while. Exiting :)")
                break
        for topic_partition, records in messages.items():
            for record in records:
                last_message_time = time.time()
                data = record.value
                #use .get() to filter interested data
                if isinstance(data, list):
                    for entry in data:
                        filtered_data = {
                            "StationName": entry.get('StationName'),
                            "StationId": entry.get('StationId'),
                            "ObservationTime": entry.get('ObsTime', {}).get('DateTime'),
                            "Weather": entry.get('WeatherElement', {}).get('Weather'),
                            "AirTemperature": entry.get('WeatherElement', {}).get('AirTemperature'),
                            "WindSpeed": entry.get('WeatherElement', {}).get('WindSpeed'),
                        }
                        print("Filtered Data:")
                        print(json.dumps(filtered_data, indent=4, ensure_ascii=False))

                else:
                    print("Unexpected data format. Expected a list.")
    consumer.close()
    print("Consumer closed :)")

if __name__ == "__main__":
    # consume_test()
    consume_weather_data()
    print("Weather data should be consumed from Kafka topic :)")