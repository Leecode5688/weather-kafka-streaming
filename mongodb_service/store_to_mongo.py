from pymongo import MongoClient, errors, UpdateOne
from motor.motor_asyncio import AsyncIOMotorClient
from config.config import TIME_OUT, MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION_NAME
import logging
import time
import os

logger = logging.getLogger(__name__)

def connect_to_mongo():

    try:
        client = MongoClient(MONGO_URI)
        client.admin.command('ping')
        logger.info("MongoDB connection successful.")
    except Exception as e:
        logger.info(f"MongoDB connection failed: {e}")
        exit(1)

    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]

    return client, collection

async def connect_to_mongo_async():
    try:
        client = AsyncIOMotorClient(MONGO_URI)
        await client.admin.command('ping')
        logger.info("Async mongodb connection successful!!")
    except Exception as e:
        logger.info(f"Async MongoDB connection failed!! {e}")
        exit(1)

    db = client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]
    return client, collection
    
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

    if not operations:
        return True
    try:
        collection.bulk_write(operations, ordered=False)
        return True
    except Exception as e:
        logger.error(f"Error during bulk write: {e}")
        return False

async def store_weather_batch_async(collection, operations: list):
    if not operations:
        return True
    try:
        await collection.bulk_write(operations, ordered=False)
        return True
    except Exception as e:
        logger.error(f"Error during async bulk write: {e}")
        return False

def close_connection(client):
    client.close()
    logger.info("MongoDB connection closed.")
