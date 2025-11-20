import logging
import os
import time
import json
import threading
import asyncio
import httpx
from config.telemetry import setup_otel
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.aiokafka import AIOKafkaInstrumentor
    
setup_otel("fetcher_service")
AIOKafkaInstrumentor().instrument()
HTTPXClientInstrumentor().instrument()

from aiokafka import AIOKafkaProducer
from config.config import FETCH_INTERVAL, FETCHER_METRICS_PORT, KAFKA_BROKER, KAFKA_RAW_TOPIC, API_URL
from config.logging_config import setup_logger
from .fetch_weather import get_weather_async
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from prometheus_client import start_http_server

logger = setup_logger("fetcher_service", "logs/fetcher.log")

tracer = trace.get_tracer(__name__)
propagator = TraceContextTextMapPropagator()

def create_producer():
    return AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

async def prepare_and_send_record(entry, producer):
    with tracer.start_as_current_span("prepare_raw_message") as span:
        try: 
            entry['fetch_timestamp'] = time.time()
            span.set_attribute("station.id", entry.get("StationId", "unknown"))
            
            #inject trace context into Kafka message headers
            headers = {}
            propagator.inject(headers)
            kafka_headers = [(k, v.encode('utf-8') if isinstance(v, str) else v) for k, v in headers.items()]
            
            await producer.send(KAFKA_RAW_TOPIC, value=entry, headers=kafka_headers)
        except Exception as e:
            logger.error(f"Failed to prepare and send record: {e}")
            span.record_exception(e)
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))

async def run_fetcher_async():
    logger.info("Starting async fetcher service...")
    producer = create_producer()
        
    async with httpx.AsyncClient() as client:
        await producer.start()
        logger.info("AIOKafkaProducer started...")
        try: 
            while True:
                with tracer.start_as_current_span("fetch_and_process_weather") as parent_span:
                    weather_data = await get_weather_async(client, API_URL)
                    
                    if weather_data:
                        logger.info(f"Fetched {len(weather_data)} weather records")
                        
                        tasks = []
                        
                        for entry in weather_data:
                            tasks.append(
                                prepare_and_send_record(entry, producer)
                            )
                                
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
    
    # Start Prometheus metrics server
    threading.Thread(target=lambda: start_http_server(FETCHER_METRICS_PORT), daemon=True).start()
    logger.info(f"Prometheus metrics server started on port {FETCHER_METRICS_PORT}")
    
    try: 
        asyncio.run(run_fetcher_async())
    except KeyboardInterrupt:
        logger.info("Fetcher service shut down.")
