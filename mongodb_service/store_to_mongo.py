from pymongo import MongoClient, errors, UpdateOne
from config.config import TIME_OUT, MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION_NAME
import logging
import time
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def connect_to_mongo():
    """
    Establish a connection to mongoDB, create compound index, return the collection object
    """
    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ping')
        logger.info("MongoDB connection successful.")
    except Exception as e:
        logger.info(f"MongoDB connection failed: {e}")
        exit(1)

    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]

    """
    compound index, ascending order
    unique = true
    ensures that the combination of StationId and ObservationTime is unique
    """

    collection.create_index(
        [("StationId", 1), ("ObservationTime", 1)],
        unique=True
    )

    return collection

def store_weather_batch(collection, data_list: list):
    """
    Store multiple weather records using bulk write for efficiency
    """
    operations = []
    
    for data in data_list:
        filtered = {
            "StationName": data.get('StationName'),
            "StationId": data.get('StationId'),
            "ObservationTime": data.get('ObsTime', {}).get('DateTime'),
            "Weather": data.get('WeatherElement', {}).get('Weather'),
            "AirTemperature": data.get('WeatherElement', {}).get('AirTemperature'),
            "WindSpeed": data.get('WeatherElement', {}).get('WindSpeed')
        }
        
        key = {"StationId": filtered["StationId"], "ObservationTime": filtered["ObservationTime"]}
        
        operations.append(UpdateOne(key, {"$setOnInsert": filtered}, upsert=True))

    if operations:
        try:
            result = collection.bulk_write(operations, ordered=False)
            logger.info(f"Bulk write result: {result.bulk_api_result}")
        except Exception as e:
            logger.error(f"Error during bulk write: {e}")

def close_connection(client):
    client.close()
    logger.info("MongoDB connection closed.")
