# Databricks Lakehouse: Streaming Medallion Architecture 🧱

![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-orange)
![PySpark](https://img.shields.io/badge/PySpark-Structured%20Streaming-red)
![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-blue)

**Repo Owner:** Burak Fenerci  
**Focus:** Data Engineering, Real-Time Pipelines, Delta Lake Optimization

## 🌊 Project Overview
This repository implements a **multi-hop streaming pipeline** adhering to the Databricks **Medallion Architecture**. It demonstrates how to turn raw, messy IoT event streams into consumption-ready "Gold" tables using **Delta Lake** and **Spark Structured Streaming**.

## 🏗️ The Medallion Architecture


1.  **🥉 Bronze (Raw):** Ingests raw JSON data. Simulates **Auto Loader** capabilities for schema evolution.
2.  **🥈 Silver (Clean):** Filters corrupt signals, handles late-arriving data (Watermarking), and removes duplicates.
3.  **🥇 Gold (Aggregated):** Calculates rolling averages for BI dashboards (e.g., "Average Temperature per Device per 5 Mins").

## 🛠️ Tech Stack
-   **PySpark:** Core transformation logic.
-   **Delta Lake:** ACID transactions and Time Travel for the data lake.
-   **Structured Streaming:** Micro-batch processing for low-latency insights.

## 🚀 How to Run (Local Simulation)
This project is structured as a Python module to demonstrate **Software Engineering best practices** for Databricks (vs. just using Notebooks).

```bash
# Install Spark & Delta dependencies
pip install -r requirements.txt

# Run the ingestion job (Simulation)
python -m src.jobs.bronze.ingest