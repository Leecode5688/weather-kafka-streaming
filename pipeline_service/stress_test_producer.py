import logging
import json
import random
import time
import os
import asyncio
import argparse
from datetime import datetime, timezone, timedelta
from config.telemetry import setup_otel
from opentelemetry.instrumentation.aiokafka import AIOKafkaInstrumentor

setup_otel("stress_test_producer")
AIOKafkaInstrumentor().instrument()

from config.config import KAFKA_BROKER, KAFKA_RAW_TOPIC 
from aiokafka import AIOKafkaProducer
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

logger = logging.getLogger("stress_producer")
logging.basicConfig(level=logging.INFO)
tracer = trace.get_tracer(__name__)
propagator = TraceContextTextMapPropagator()

def create_producer():
    logger.info(f"Connecting to Kafka Broker at: {KAFKA_BROKER}") 
    return AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        request_timeout_ms=60000 
    )
    
    
def generate_fake_weather_data():
    station_ids = ["FA001", "FA002", "FA003", "FA004", "FA005"]
    station_names = ["NTU", "Taipei", "New Taipei", "Kaohsiung", "Taichung"]
    station_index = random.randint(0, len(station_ids) - 1)
    taipei_tz = timezone(timedelta(hours=8))
    
    return {
            "fetch_timestamp": time.time(),
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
    
async def send_one_message(producer, message):
    with tracer.start_as_current_span("stress_test_send") as span:
        try:
            span.set_attribute("station.id", message.get("StationId", "unknown"))
            
            headers = {}
            propagator.inject(headers)
            kafka_headers = [(k, v.encode('utf-8') if isinstance(v, str) else v) for k, v in headers.items()]

            await producer.send(KAFKA_RAW_TOPIC, value=message, headers=kafka_headers)
        except Exception as e:
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            logger.error(f"Failed to send stress test message: {e}")
            
            
async def run_stress_test(num_messages):
    producer = create_producer()
    logger.info(f"Starting stress test, preparing to send {num_messages} messages in batches...")
    
    await producer.start()
    tasks = []
    with tracer.start_as_current_span("stress_test_run") as parent_span:
        parent_span.set_attribute("test.num_messages", num_messages)
        try: 
            for i in range(num_messages):
                message = generate_fake_weather_data()
                tasks.append(
                    send_one_message(producer, message)
                )
                
                if (i + 1) % 1000 == 0 or (i + 1) == num_messages:
                    logger.info(f"Sending batch ending at message {i + 1}/{num_messages}...")
                    await asyncio.gather(*tasks) 
                    tasks.clear() 
            
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            parent_span.record_exception(e)
            parent_span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
        finally:
            await producer.stop()
            logger.info(f"Stress test finished! Sent {num_messages} messages...")

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Kafka stress test producer.")
    parser.add_argument(
        "num_messages", 
        type=int, 
        nargs="?",
        default=10000,
        help="The number of messages to send (default: 10000)"
    )
    args = parser.parse_args()

    asyncio.run(run_stress_test(args.num_messages))
