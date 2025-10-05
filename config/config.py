import os
from dotenv import load_dotenv

#load .env from project root
load_dotenv(dotenv_path="config/.env")

#load weather api config
API_KEY = os.getenv("CWB_API_KEY")
API_URL = f"https://opendata.cwa.gov.tw/api/v1/rest/datastore/O-A0001-001?Authorization={API_KEY}"
STATION_IDS_OF_INTEREST = ["C0C730"]
#how often do we call the API
FETCH_INTERVAL = int(os.getenv("FETCH_INTERVAL", 10))
RUN_DURATION = int(os.getenv("RUN_DURATION", 600))

#mongodb config
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME")

#kafka config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_data")
TIME_OUT = int(os.getenv("TIME_OUT", 600))  # seconds


