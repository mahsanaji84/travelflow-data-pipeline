# TravelFlow Data Pipeline

TravelFlow is an end-to-end data engineering project that demonstrates both **real-time streaming** and **batch processing** workflows using modern data tools.

## Architecture

### Real-time pipeline

```text
Python Simulator
      ↓
Apache Kafka
      ↓
Spark Structured Streaming
      ↓
Apache Cassandra
