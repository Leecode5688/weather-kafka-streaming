import logging
import os
import time
import json
import threading
import asyncio
import httpx
from .fetch_weather import get_weather_async
from config.config import FETCH_INTERVAL, FETCHER_METRICS_PORT, KAFKA_BROKER, KAFKA_RAW_TOPIC, API_URL
from config.logging_config import setup_logger
from config.telemetry import setup_otel
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.aiokafka import AIOKafkaInstrumentor
from opentelemetry import trace
from prometheus_client import start_http_server
from aiokafka import AIOKafkaProducer

logger = setup_logger("fetcher_service", "logs/fetcher.log")

tracer = trace.get_tracer(__name__)

def create_producer():
    return AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

async def run_fetcher_async():
    logger.info("Starting async fetcher service...")
    producer = create_producer()
        
    async with httpx.AsyncClient() as client:
        await producer.start()
        logger.info("AIOKafkaProducer started...")
        try: 
            while True:
                weather_data = await get_weather_async(client, API_URL)
                
                if weather_data:
                    logger.info(f"Fetched {len(weather_data)} weather records")
                    
                    tasks = []
                    
                    for entry in weather_data:
                        try:
                            entry['fetch_timestamp'] = time.time()
                            tasks.append(
                                producer.send(KAFKA_RAW_TOPIC, value=entry)
                            )
                        except Exception as e:
                            logger.error(f"Failed to create and send task!! {e}")
                            
                    if tasks:
                        await asyncio.gather(*tasks)
                        logger.info(f"Flushed {len(tasks)} raw weather records to {KAFKA_RAW_TOPIC}")

                else:
                    logger.warning("No weather data fetched this interval")
                
                await asyncio.sleep(FETCH_INTERVAL)    
                
        except KeyboardInterrupt:
            logger.info("Fetcher service stopped by user.")
        finally: 
            producer.close()
            logger.info("Fetcher kafka producer closed.")
            
                
if __name__ == "__main__":
    
    setup_otel("fetcher_service")
    AIOKafkaInstrumentor().instrument()
    HTTPXClientInstrumentor().instrument()

    # Start Prometheus metrics server
    threading.Thread(target=lambda: start_http_server(FETCHER_METRICS_PORT), daemon=True).start()
    logger.info(f"Prometheus metrics server started on port {FETCHER_METRICS_PORT}")
    
    try: 
        asyncio.run(run_fetcher_async())
    except KeyboardInterrupt:
        logger.info("Fetcher service shut down.")
