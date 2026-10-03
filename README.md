# TravelFlow Data Pipeline

![TravelFlow CI](https://github.com/mahsanaji84/travelflow-data-pipeline/actions/workflows/ci.yml/badge.svg)

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
