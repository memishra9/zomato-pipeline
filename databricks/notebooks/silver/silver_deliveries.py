# Databricks notebook source
from pyspark.sql.functions import   col, from_json
from pyspark.sql.types import StructType,StructField,ArrayType,StringType,DoubleType,TimestampType
bronze_df=spark.read.table("zomato_bronze.raw_events")
deliveries_raw = bronze_df.filter(col("kafka_topic")=="deliveries")

# COMMAND ----------

deliveries_raw.select("kafka_value").display(truncate=False)

# COMMAND ----------

schema=StructType([
    StructField("event_id",StringType(),False),
    StructField("event_type",StringType(),False),
    StructField("timestamp",TimestampType(),False),
    StructField("user_id",StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("driver_id",StringType(),False),
    StructField("delivery_time",TimestampType(),False)
])

deliveries_parsed=deliveries_raw.withColumn("data", from_json(col("kafka_value"),schema))

# COMMAND ----------

deliveries_parsed.select("data").show(5,truncate=False)

# COMMAND ----------

deliveries_parsed.printSchema()


# COMMAND ----------

deliveries_parsed.select(
    col("data.event_id"),
    col("data.event_type"),
    col("data.timestamp"),
    col("data.user_id"),
    col("data.order_id"),
    col("data.driver_id"),
    col("data.delivery_time"),
    col("kafka_partition"),
    col("kafka_offset"),
    col("kafka_timestamp"),
    col("kafka_topic")
).dropna(subset=["event_id", "event_type", "order_id", "driver_id"]) \
 .dropDuplicates(["event_id"]) \
 .write.mode("append").format("delta") \
 .saveAsTable("zomato_silver.deliveries")

# COMMAND ----------

spark.read.table("zomato_silver.deliveries").count()