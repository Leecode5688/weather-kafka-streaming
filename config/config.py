import os
from dotenv import load_dotenv

#load .env from project root
load_dotenv(dotenv_path="config/.env")

#load weather api config
API_KEY = os.getenv("CWB_API_KEY")
API_URL = f"https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0001-001?Authorization={API_KEY}"
STATION_IDS_OF_INTEREST = ["C0C730", "C0C790", "C0AJ80", "CAAH60"]
#how often do we call the API
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", 10))
RUN_DURATION = int(os.getenv("RUN_DURATION", 3600))

#mongodb config
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

#kafka config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

#pipeline 1: raw fetcher => processor
KAFKA_RAW_TOPIC = os.getenv("KAFKA_RAW_TOPIC", "weather_raw")
KAFKA_PIPELINE_DLQ_TOPIC = os.getenv("KAFKA_PIPELINE_DLQ_TOPIC", "weather_raw_dlq")

#pipeline 2: processor => consumer
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_data")
KAFKA_CONSUMER_DLQ_TOPIC = os.getenv("KAFKA_CONSUMER_DLQ_TOPIC", "weather_data_dlq")

TIME_OUT = int(os.getenv("TIME_OUT", 600))
BATCH_TIMEOUT = int(os.getenv("BATCH_TIMEOUT", 5))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 2000))

#prometheus metrics port
CONSUMER_METRICS_PORT = int(os.getenv("CONSUMER_METRICS_PORT", 8000))
PRODUCER_METRICS_PORT = int(os.getenv("PRODUCER_METRICS_PORT", 8001))
FETCHER_METRICS_PORT = int(os.getenv("FETCHER_METRICS_PORT", 8002))
