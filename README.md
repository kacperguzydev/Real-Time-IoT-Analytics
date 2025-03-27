Real-Time IoT Analytics for Smart Building Energy Management
This repository contains a real-time data pipeline for monitoring and analyzing building energy usage. It demonstrates how to:

Ingest IoT sensor data via Kafka

Process data in real time with Spark Structured Streaming

Store the processed data in PostgreSQL

Visualize insights through a Streamlit dashboard

Features
Kafka Producer to simulate or forward real-time sensor data

Spark Consumer (spark_consumer.py) that reads from Kafka, cleans data, and writes to PostgreSQL

PostgreSQL Database setup for storing building metadata, weather data, and energy readings

Streamlit Dashboard (visualizer.py) for real-time or historical analytics
