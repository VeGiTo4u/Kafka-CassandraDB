# **Kafka to Cassandra Streaming Pipeline**

This project demonstrates a real-time data streaming pipeline where order data from a CSV file is published to Kafka using a custom producer, consumed by multiple consumers running inside Docker containers, and finally ingested into Cassandra DB hosted on AstraDB.
The pipeline is designed to simulate real-world streaming use cases, ensuring scalability, consistency, and reliability through Kafka’s parallel consumers and Cassandra’s quorum consistency level.

⸻

# **Project Overview**
	1.	Producer Code
	•	Reads order data from a CSV file (orders.csv).
	•	Publishes records into a Kafka topic.
	2.	Consumer Code (Dockerized)
	•	Multiple consumer instances are created and managed using Docker.
	•	Consumers subscribe to the Kafka topic and process incoming order messages.
	•	Processed data is written into Cassandra DB.
	3.	Cassandra DB (AstraDB)
	•	Used AstraDB (managed Cassandra service) to host the database.
	•	Created tables with appropriate primary keys and applied quorum consistency for reliable reads/writes.
	•	Established connection between Kafka consumers and AstraDB for ingestion.

⸻

# **Features**
	•	End-to-end streaming pipeline: CSV → Kafka → Cassandra.
	•	Dockerized consumers: Parallel consumption and scalability.
	•	Cassandra quorum consistency: Ensures reliable and fault-tolerant writes.
	•	Managed Cassandra (AstraDB): Simplified cluster management and easy integration.
	•	Hands-on with producer & consumer code: Custom implementations for both.

⸻

# **Tech Stack**
	•	Apache Kafka – Streaming platform for producers/consumers.
	•	Docker – To containerize consumer instances and run in parallel.
	•	Cassandra DB (AstraDB) – Managed Cassandra service for storage.
	•	Python – For producer and consumer implementation.
	•	CSV – Source data format.

⸻

# **Project Flow**
	1.	Orders Data (CSV)
	•	Source data stored in orders.csv.
	2.	Kafka Producer
	•	Reads from CSV and publishes order records into a Kafka topic.
	3.	Kafka Topic
	•	Acts as the streaming channel where records are queued.
	4.	Dockerized Kafka Consumers
	•	Multiple consumer instances (via Docker) subscribe to the topic.
	•	Each consumer reads messages in parallel and prepares them for ingestion.
	5.	Cassandra DB (AstraDB)
	•	Consumers write processed data into Cassandra tables.
	•	Quorum consistency ensures reliable reads/writes.
	•	Data is stored for further querying and analytics.

# **Repository Structure**
```
ecommerce-kafka-cassandra/
│
├── dataset/
│   └── olist_orders_data.csv          # Source dataset (orders data)
│
├── scripts/
│   ├── producer.py                    # Producer script (reads CSV, publishes to Kafka)
│   ├── ecommerceorders.ipynb          # Jupyter notebook for analysis/experiments
│   │
│   └── consumer_group/                # Consumer group setup with Docker
│       ├── Dockerfile                 # Docker image for consumer
│       ├── consumer.py                # Consumer script (reads Kafka, writes to Cassandra)
│       ├── docker-compose.yml         # Docker compose for scaling consumer instances
│       └── orders-demo.ipynb          # Notebook for testing/validation
│
├── avro_schema.avsc                   # Avro schema for orders data
│
├── LICENSE                            # License file
└── README.md                          # Project documentation
```


# **Key Learning Outcomes**
	•	Writing custom producers and consumers for Kafka.
	•	Dockerizing consumers to achieve horizontal scalability.
	•	Using AstraDB (Cassandra) for real-time ingestion and managing quorum consistency.
	•	Understanding how data flows from raw CSV → streaming → NoSQL database.

 # **License**

This project is open-source and available under the MIT License.
