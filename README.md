# Kafka-based Weather Observation Streaming Project

This project implements a kafka-based weather data streaming system. It fetches hourly meteorological data from Taiwan's Central Weather Administration (CWA) API, streams it through a multi-stage **Apache Kafka** pipeline, and stores it in a sharded **MongoDB** cluster. 

The pipeline features a **Dead Letter Queue (DLQ)** mechanism to catch and isolate any malformed data, ensuring the main data flow is never interrupted and no data is lost. This project also uses **Prometheus** and **Grafana** for monitoring. 

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
- Fetcher Service (weather_fetcher): Responsible for fetching data from CWA API at regular intervals and producing it to the weather_raw Kafka topic. 
- Pipeline Service (weather_pipeline): A Kafka-streams-style processor that consumes from the weather_raw topic, performs validation and light transformations, and produces the cleaned data to the weather_data topic.
- Consumer Service (weather_consumer): Consumes cleaned data from the weather_data topic in batches, perform final validation, and performs bulk upserts into the MongoDB cluster. 
- MongoDB Sharded Cluster: A horizontally scalable database setup consisting of config servers, shard servers, and a query router (mongos-router) to store the final weather data. The collection is sharded by a hashed key on StationId. 
- Dead Letter Queues (DLQs): Two dedicated topics (weather_raw_dlq and weather_data_dlq) that capture any messages failing validation at either pipeline or consumer stage, preventing data loss. 
- Kafka: The core of the streaming platform, decoupling the services.
- Prometheus & Grafana: Scrape and visualize application metrics from these Python services.
- Docker & Docker compose: Containerizes and archestrates this multi-service project. 

The data flows through the system as follows: 
- Valid data path: CWA API => Fetcher Service => Kafka (weather_raw topic) => Pipeline Service => Kafka (weather_data topic) => Consumer Service => MongoDB Sharded Cluster

- Error Data Paths (DLQ): 
	- Path 1: weather_raw => pipeline service (fails during validation) => Kafka (weather_raw_dlq)
	- Path 2: weather_data => Consumer service (fails during validation) => Kafka (weather_data_dlq)

## How to Run the Project
Prerequisites: Docker, Docker compose

### Create a .env file: 
In the `config/` directory, create a file named `.env`. Copy the following content into it and replace `<Your_CWA_API_Key>` with your actual API key.
    
    ```
    CWB_API_KEY=<Your_CWA_API_Key>

	FETCH_INTERVAL=600
	RUN_DURATION=3600
	MONGO_URI=mongodb://mongos-router:27017,mongos-router-2:27017/
	MONGO_DB_NAME=weather_db
	MONGO_COLLECTION_NAME=weather_data
	KAFKA_BROKER=kafka:9092
	KAFKA_TOPIC=weather_data
	KAFKA_RAW_TOPIC=weather_raw
	KAFKA_PIPELINE_DLQ_TOPIC=weather_raw_dlq
	KAFKA_CONSUMER_DLQ_TOPIC=weather_data_dlq

	TIME_OUT=1800
	BATCH_TIMEOUT=5
	BATCH_SIZE=500
	LOG_FILE=logs/pipeline.log
	LOG_LEVEL=INFO

	CONSUMER_METRICS_PORT=8000
	PRODUCER_METRICS_PORT=8001
	FETCHER_METRICS_PORT=8002

	OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4317

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
- Jaeger UI: `http://localhost:16686`
- MongoDB: Connect at `mongodb://localhost:27017`

### Verify Data in MongoDB Shell

To quickly verify that data is being stored, you can connect to the MongoDB container's shell:

1. Open the shell:
    ```
    docker exec -it mongos-router mongosh
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
## Validate the Dead Letter Queues
We can use the Kafka UI (http://localhost:8080) to manually send malformed messages and confirm they are caught.

### Test the Pipeline Service DLQ:
Navigate to the weather_raw topic.
Produce a message that is not valid JSON (e.g., not json).
Produce another message that is JSON but missing fetch_timestamp (e.g., {"hello": "world"}).
Verify: Go to the weather_raw_dlq topic. You will see both bad messages here. They will not appear in the weather_data topic.

### Test the Consumer Service DLQ:
Navigate to the weather_data topic.
Produce a message that is valid JSON but missing a required DB field like StationId (e.g., {"timestamp": 123, "ObsTime": "2025-10-27T16:00:00"}).
Verify: Go to the weather_data_dlq topic. You will see this message here. It will not be in your MongoDB database.

## Stress Testing the Pipeline

1. Ensure all services are running.
2. Execute the stress test script:    
    ```
    docker exec -it weather_pipeline python -m producer_service.stress_test_producer
    ```
    
	This command runs a script inside the `weather_pipeline` container to generate a 10000 test data.
    
3. Monitor the pipeline in Grafana to observe system performance.

Note: In this stress test the DLQ topics should remain empty, as this test script only sends valid data. 

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