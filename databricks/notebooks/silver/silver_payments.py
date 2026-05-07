# Databricks notebook source
from pyspark.sql.functions import   col, from_json
from pyspark.sql.types import StructType,StructField,ArrayType,StringType,DoubleType,TimestampType
bronze_df=spark.read.table("zomato_bronze.raw_events")
payments_raw = bronze_df.filter(col("kafka_topic")=="payments")

# COMMAND ----------

payments_raw.select("kafka_value").show(truncate=False)
#payments_raw.printSchema()

# COMMAND ----------

payment_schema = StructType([
    StructField("event_id" ,StringType(), False),
    StructField("event_type",StringType(), False),
    StructField("timestamp",TimestampType(), False),
    StructField("user_id",StringType(), False),
    StructField("payment_id", StringType(), False),
    StructField("order_id",StringType(), False),
    StructField("amount",DoubleType(), False),
    StructField("payment_method",StringType(), False)
])

payment_parsed = payments_raw.withColumn("data", from_json(col("kafka_value"), schema=payment_schema))

payment_parsed.printSchema()

# COMMAND ----------

payments_silver=payment_parsed.select(
    col("data.event_id"),
    col("data.event_type"),
    col("data.timestamp"),
    col("data.user_id"),
    col("data.payment_id"),
    col("data.order_id"),
    col("data.amount"),


    col("kafka_partition"),
    col("kafka_offset"),
    col("kafka_timestamp"),
    col("data.payment_method")
    )


payments_silver.show()

# COMMAND ----------

payments_silver.dropna(subset=["event_id", "event_type", "order_id", "user_id", "amount", "payment_method"]).dropDuplicates(["event_id"]).write.mode("append").format("delta").saveAsTable("zomato_silver.payments")


# COMMAND ----------

spark.read.table("zomato_silver.payments").count()