import json
import logging
import random
import time
from datetime import datetime, timezone, timedelta
from config.config import KAFKA_BROKER, KAFKA_TOPIC
from kafka import KafkaProducer

logger = logging.getLogger("stress_producer")
logging.basicConfig(level=logging.INFO)

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER, 
        api_version=(3, 9),
        value_serializer=lambda v : json.dumps(v).encode('utf-8'),
    )
    
def generate_fake_weather_data():
    station_ids = ["FA001", "FA002", "FA003", "FA004", "FA005"]
    station_names = ["NTU", "Taipei", "New Taipei", "Kaohsiung", "Taichung"]
    station_index = random.randint(0, len(station_ids) - 1)
    taipei_tz = timezone(timedelta(hours=8))
    
    return {
            "timestamp": time.time(),
            "StationId": station_ids[station_index],
            "StationName": station_names[station_index],
            "ObsTime": {
                "DateTime": datetime.now(taipei_tz).isoformat()
            },
            "WeatherElement": {
                "Weather": "Sunny",
                "AirTemperature": round(random.uniform(15.0, 35.0), 1),
                "WindSpeed": round(random.uniform(0.0, 20.0), 1)
            }
        }
    
def run_stress_test(num_messages=10000):
    producer = create_producer()
    logger.info(f"Starting stress test, preparing to send {num_messages} messages. ")
    try: 
        for i in range(num_messages):
            message = generate_fake_weather_data()
            producer.send(KAFKA_TOPIC, value = message)
            
            if (i+1) % 1000 == 0: 
                logger.info(f"Sent {i+1}/{num_messages} messages...")
                
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        producer.flush()
        producer.close()
        logger.info(f"Stress test finished! Sent {num_messages} messages...")

if __name__ == "__main__":
    run_stress_test()