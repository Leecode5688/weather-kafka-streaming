# Kafka-based Weather Observation Streaming Project
## Project Description

This basic project is a real-time **weather observation streaming system** that uses **Apache Kafka** to stream weather data from CWB Taiwan API to **MongoDB** database.

**Key Features**:
- **Kafka Producer** to stream weather data to Kafka.
- **Kafka Consumer** to consume weather data and store it into MongoDB.
- **MongoDB** for storing weather data with deduplication (ensured by compound unique indexes).