# Databricks notebook source
# MAGIC %sql
# MAGIC drop table if exists zomato_silver.ratings

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
bronze_df=spark.read.table("zomato_bronze.raw_events")
ratings_raw=bronze_df.filter(col("kafka_topic")=="ratings")

# COMMAND ----------

ratings_raw.select("kafka_value").show(truncate=False)

# COMMAND ----------

schema=StructType([
    StructField("event_id",StringType(),False),
    StructField("event_type",StringType(),False),
    StructField("timestamp",TimestampType(),False),
    StructField("user_id",StringType(),False),
    StructField("order_id",StringType(),False),
    StructField("rating",IntegerType(),False),
    StructField("restaurant_id",StringType(),False),


    
])

# COMMAND ----------

ratings_parsed=ratings_raw.withColumn("data", from_json(col("kafka_value"),schema))

# COMMAND ----------

ratings_parsed.select(
    col("data.event_id"),
    col("data.event_type"),
    col("data.timestamp"),
    col("data.user_id"),
    col("data.order_id"),
    col("data.rating"),
    col("data.restaurant_id"),
    col("kafka_partition"),
    col("kafka_offset"),
    col("kafka_timestamp"),
    col("kafka_topic")
).dropna(subset=["event_id", "event_type", "order_id", "rating","restaurant_id"]) \
 .dropDuplicates(["event_id"]) \
 .write.mode("append").format("delta") \
 .saveAsTable("zomato_silver.ratings")

# COMMAND ----------

spark.read.table("zomato_silver.ratings").count()