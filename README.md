# Real-Time Logistics & Inventory Monitoring System

## Author
Nagesh  
Email: nageshbachanagoni@gmail.com  
LinkedIn: www.linkedin.com/in/nagesh-bachanagoni-b84346153  
Portfolio: https://bachanagoninagesh.github.io/

## Project Overview
This project implements a real-time data pipeline to simulate and process logistics data, including inventory updates and driver GPS events. The system uses a distributed architecture to ingest, process, store, and visualize streaming data.

## Architecture
Python Producers → Kafka → Spark Structured Streaming → PostgreSQL → Power BI

## Key Components
- **Kafka Producers**: Generate real-time inventory and driver GPS events  
- **Kafka Topics**: `inventory_events`, `driver_gps_events`  
- **Spark Streaming**: Processes and transforms streaming data  
- **PostgreSQL**: Stores processed data in fact tables  
- **Power BI**: Visualizes real-time insights using DirectQuery  

## Features
- Real-time data ingestion and processing  
- Multi-stream handling (inventory and GPS data)  
- Fault-tolerant streaming with checkpointing  
- Containerized deployment using Docker  
- Live dashboard for monitoring logistics data  

## Tech Stack
Python, Apache Kafka, Apache Spark, PostgreSQL, Docker, Power BI
