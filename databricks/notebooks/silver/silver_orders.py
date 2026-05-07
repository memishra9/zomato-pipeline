# Databricks notebook source
from pyspark.sql.functions import col, to_timestamp,from_json
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,ArrayType
bronze_df=spark.read.table('zomato_bronze.raw_events')
orders_raw=bronze_df.filter(col("kafka_topic")=="orders")

# COMMAND ----------

order_schema= StructType([
    StructField("event_id",StringType(),False),
    StructField("event_type", StringType(), False),
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(),True),
    StructField("order_id", StringType(), True),
    StructField("restaurant_id",StringType(), True),
    StructField("city", StringType(), True),
    StructField("items", ArrayType(StringType()), True),
    StructField("price",DoubleType(), True)


    ]
)

order_parsed=orders_raw.withColumn("data", from_json(col("kafka_value"),schema=order_schema))
order_parsed.select("data").show(truncate=False)

# COMMAND ----------

orders_silver= order_parsed.select(
    col("data.event_id"),
    col("data.event_type"),
    to_timestamp(col("data.timestamp")).alias("timestamp"),
    col("data.user_id"),
    col("data.order_id"),
    col("data.restaurant_id"),
    col("data.city"),
    col("data.items"),
    col("data.price"),
    col("kafka_partition"),
    col("kafka_offset"),
    col("kafka_timestamp")
)

# COMMAND ----------

(
    orders_silver
    .dropna(subset=["event_id", "event_type", "order_id", "user_id", "restaurant_id", "items", "price"])
    .dropDuplicates(["event_id"])
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("zomato_silver.orders")
)


# COMMAND ----------

spark.read.table("zomato_silver.orders").show()

# COMMAND ----------

# Validatiom
bronze_count=spark.read.table("zomato_bronze.raw_events").filter(col("kafka_topic")=="orders").count()
silver_count= spark.read.table("zomato_silver.orders").count()
print("Bronze count: ", bronze_count)
print("Silver count: ", silver_count)

# COMMAND ----------

#validation
critical_cols = ["event_id", "order_id", "user_id", "restaurant_id", "price"]

for c in critical_cols:
    null_count = spark.read.table("zomato_silver.orders").filter(col(c).isNull()).count()
    print(f"{c} nulls: {null_count}")

