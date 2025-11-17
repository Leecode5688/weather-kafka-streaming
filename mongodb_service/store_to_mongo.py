from pymongo import MongoClient, errors, UpdateOne
from motor.motor_asyncio import AsyncIOMotorClient
from config.config import TIME_OUT, MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION_NAME
import logging
import time
import os

logger = logging.getLogger(__name__)

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
