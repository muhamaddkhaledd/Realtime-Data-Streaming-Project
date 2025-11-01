# 🌀 Realtime Data Streaming Pipeline

This project demonstrates a **fully containerized, end-to-end real-time data engineering pipeline** built with **Apache Airflow**, **Kafka**, **Zookeeper**, **Spark**, **Cassandra**, **PostgreSQL**, and **Docker**.  
It covers the complete data flow — from ingestion and orchestration to streaming, processing, and storage — ensuring scalability, automation, and reliability.

---

## 🧩 Project Architecture

Below is the architecture of the project pipeline 👇  

![Architecture Diagram](Realtime-Data-Streaming-architecture.png)

---

## ⚙️ Workflow Overview

1. **Data Ingestion (Apache Airflow + PostgreSQL)**  
   - Airflow orchestrates the pipeline and fetches random user data from the [`randomuser.me`](https://randomuser.me) API.  
   - The fetched data is stored in a PostgreSQL database.

2. **Streaming (Kafka + Zookeeper)**  
   - Kafka streams data in real time from PostgreSQL to Spark for processing.  
   - Zookeeper handles distributed coordination and synchronization.

3. **Processing (Apache Spark)**  
   - Spark (master and worker nodes) processes the streaming data — cleaning, transforming, and enriching it.  

4. **Storage (Cassandra)**  
   - The processed data is written to Cassandra, a highly scalable NoSQL database optimized for fast reads and writes.

5. **Containerization (Docker)**  
   - Every service runs in its own Docker container for easy deployment, scalability, and isolation.

---

## 🧠 Tech Stack

| Component | Technology |
|------------|-------------|
| **Orchestration** | Apache Airflow |
| **Data Streaming** | Apache Kafka |
| **Coordination** | Apache Zookeeper |
| **Processing Engine** | Apache Spark |
| **Databases** | PostgreSQL, Cassandra |
| **Containerization** | Docker |
| **Language** | Python |

---
