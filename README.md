# Kafka-based Weather Observation Streaming Project

This project implements a kafka-based weather data streaming system. It fetches hourly meteorological data from Taiwan's Central Weather Administration (CWA) API, streams it to **Apache Kafka**, and stores it in **MongoDB**. This project also uses **Prometheus** and **Grafana** for monitoring. 

## A Brief Introduction of Apache Kafka

Apache Kafka is an open source distributed streaming platform widely used in real time data pipelines. It is known for its horizontal scalability, high throughput and fault tolerance.
Kafka often serves as the central nervous system of data driven architectures, enabling loosely coupled communication between services that need to handle massive amount of data. 

### Core Components of Kafka

- **Events**: 
	An event (or message) of kafka is the data that we care about. It can be a website click, readings from IoT sensors, financial transactions, etc. In Kafka, events are organized and stored in topics. 
	
- **Producers**: 
	A producer is a client application that publishes (writes) events to a specific topic in the Kafka cluster. 
	
- **Topics**: 
	A topic is where events are published. It's a logical way to categorize data. Topics in Kafka are divided into partitions, meaning a topic is spread across multiple "buckets" located on different Kafka brokers. 
	
- **Partitions**: 
	A topic is divided into one or more partitions. These partitions are the actual, physical logs where the event data is stored in an ordered, immutable sequence. 
	
- **Brokers**: 
	A broker is a single Kafka server. A Kafka cluster is composed of multiple brokers working together. Each broker hosts a set of partitions. 
	
- **Consumers**: 
	A consumer is a client application that subscribes to (reads and processes) events from a Kafka topic. 
	
- **Consumer Groups**: 
	Consumers can be organized into consumer groups (by configuring with the same group_id). Different consumer groups in Kafka often represent different services or subsystems that need to process the same stream of data independently. Kafka distributes the topic partitions among the consumers in that group. This means that each partition is consumed by only one member in the group, which prevents messages from being processed multiple times.  

#### Data Flow Summary
1. Producer sends messages to a topic.
2. Kafka assigns each message to a partition.
3. Partitions are stored across brokers. 
4. Consumers in a group read from assigned partitions using offsets. 
5. Kafka tracks offsets per consumer group, enabling fault tolerance and replay. 

## Architecture Overview
This project consists of the following key components: 
- Fetcher Service: Responsible for fetching data from CWA API at regular intervals. 
- Kafka Producer: Takes raw weather data and produces it to a kafka topic.
- Kafka Consumer: Consumes weather data from the topic (weather_data in this repo) in batches and stores it in MongoDB.
- MongoDB: Stores the weather data. In this project it's configured with a compound unique index to prevent duplicate entries.
- Kafka: The core of the streaming platform.
- Prometheus & Grafana: Scrape and visualize application metrics.
- Docker & Docker compose: Containerizes this project. 
The data flows through the system as follows: 

CWA weather API => Fetcher service => Kafka => Consumer service => MongoDB
## How to Run the Project
Prerequisites: Docker, Docker compose

### Create a .env file: 
In the `config/` directory, create a file named `.env`. Copy the following content into it and replace `<Your_CWA_API_Key>` with your actual API key.
    
    ```
    CWB_API_KEY=<Your_CWA_API_Key>
	FETCH_INTERVAL=600
	RUN_DURATION=3600
	MONGO_URI=mongodb://mongodb:27017/weather_db
	MONGO_DB_NAME=weather_db
	MONGO_COLLECTION_NAME=weather_data
	KAFKA_BROKER=kafka:9092
	KAFKA_TOPIC=weather_data
	TIME_OUT=1800
	BATCH_TIMEOUT=5
	BATCH_SIZE=500
	LOG_FILE=logs/pipeline.log
	LOG_LEVEL=INFO
	
	CONSUMER_METRICS_PORT=8000
	PRODUCER_METRICS_PORT=8001
	FETCHER_METRICS_PORT=8002

    ```

### Running the Application

1. Start the services:
    ```
    docker compose -f docker/docker-compose.yml up -d
    ```
2. Verify the services are running:
    ```
    docker compose -f docker/docker-compose.yml ps
    ```
### Accessing the Services

- Kafka UI: `http://localhost:8080`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (login: `admin`/`admin`)
- MongoDB: Connect at `mongodb://localhost:27017`

### Verify Data in MongoDB Shell

To quickly verify that data is being stored, you can connect to the MongoDB container's shell:

1. Open the shell:
    ```
    docker exec -it mongodb mongosh
    ```
    
2. Switch to the correct database:
    
    ```
    use weather_db
    ```
    
3. Find one document in the collection:
    ```
    db.weather_data.findOne()
    ```
    
    This will display a single weather data document, confirming the pipeline is working.

4.  See the total number of documents that is being stored: 
	```
	db.weather_data.countDocuments()
	```
## Stress Testing the Pipeline

1. Ensure all services are running.
    
2. Execute the stress test script:    
    ```
    docker exec -it weather_pipeline python -m    producer_service.stress_test_producer
    ```
    
	This command runs a script inside the `weather_pipeline` container to generate a 10000 test data.
    
3. Monitor the pipeline in Grafana to observe system performance.

## Monitoring with Grafana

### Add Prometheus as a Data Source

1. Navigate to Grafana at `http://localhost:3000`.
2. Go to Connections > Data Sources > Add new data source.
3. Select Prometheus.
4. Set the URL to `http://prometheus:9090`.
5. Click Save & test.
    

### Example Queries for Dashboard Panels

- Message Consumption Rate: `rate(consumer_messages_consumed_total[5m])`
- Message Production Rate: `rate(producer_messages_sent_total[5m])`
- P95 Message Latency (seconds): `histogram_quantile(0.95, sum(rate(consumer_message_latency_seconds_bucket[5m])) by (le))`
- API Call Rate: `rate(fetcher_api_calls_success_total[5m])`