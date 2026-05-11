# Zomato Analytics Pipeline

## Architecture
# Zomato Analytics Pipeline

## Architecture
# Zomato Analytics Pipeline

## Architecture
<img width="756" height="274" alt="image" src="https://github.com/user-attachments/assets/c8570e0e-0065-4775-8117-f824aaad9fd7" />


## Tech Stack
- Confluent Kafka Cloud
- Python (confluent-kafka, faker)
- Apache Spark Structured Streaming
- Databricks Community Edition
- Delta Lake (medallion architecture)

## Pipeline Layers
- Bronze: raw events from Kafka
- Silver: cleaned, typed, deduplicated
- Gold: aggregated business metrics

## Event Types
- order_placed, payment_done, driver_assigned
- order_delivered, rating_given

## Gold Metrics
- Hourly order metrics by city
- Restaurant performance
- Delivery funnel duration

## Setup
<producer setup steps>
<Databricks setup steps>

## Future Improvements
- Airflow orchestration
- dbt for Gold transforms
- pytest coverage
- MERGE for deduplication
## Tech Stack
- Confluent Kafka Cloud
- Python (confluent-kafka, faker)
- Apache Spark Structured Streaming
- Databricks Community Edition
- Delta Lake (medallion architecture)

## Pipeline Layers
- Bronze: raw events from Kafka
- Silver: cleaned, typed, deduplicated
- Gold: aggregated business metrics

## Event Types
- order_placed, payment_done, driver_assigned
- order_delivered, rating_given

## Gold Metrics
- Hourly order metrics by city
- Restaurant performance
- Delivery funnel duration

## Setup
<producer setup steps>
<Databricks setup steps>

## Future Improvements
- Airflow orchestration
- dbt for Gold transforms
- pytest coverage
- MERGE for deduplication

## Tech Stack
- Confluent Kafka Cloud
- Python (confluent-kafka, faker)
- Apache Spark Structured Streaming
- Databricks Community Edition
- Delta Lake (medallion architecture)

## Pipeline Layers
- Bronze: raw events from Kafka
- Silver: cleaned, typed, deduplicated
- Gold: aggregated business metrics

## Event Types
- order_placed, payment_done, driver_assigned
- order_delivered, rating_given

## Gold Metrics
- Hourly order metrics by city
- Restaurant performance
- Delivery funnel duration

## Setup
<producer setup steps>
<Databricks setup steps>

## Future Improvements
- Airflow orchestration
- dbt for Gold transforms
- pytest coverage
- MERGE for deduplication
