#This is databricks Notebook

#Getting secrets in databricks
bootstrap_servers = dbutils.secrets.get(
    scope="kafka-secrets", key="KAFKA_BOOTSTRAP_SERVERS"
)
api_key = dbutils.secrets.get(scope="kafka-secrets", key="KAFKA_API_KEY")
api_secret = dbutils.secrets.get(scope="kafka-secrets", key="KAFKA_API_SECRET")



dbutils.secrets.list(scope="kafka-secrets")


#config details same as consumer
kafka_options = {
    "kafka.bootstrap.servers": bootstrap_servers,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";',
    "startingOffsets": "earliest",
    "failOnDataLoss": "false",
}


#kafka consumer code
raw_stream = (
    spark.readStream.format("kafka")
    .options(**kafka_options)
    .option("subscribe", "orders,deliveries,payments,ratings")
    .load()
)



raw_stream.printSchema()



from pyspark.sql.functions import col, cast

bronze_stream = raw_stream.select(
    col("key").cast("string").alias("kafka_key"),
    col("value").cast("string").alias("kafka_value"),
    col("topic").alias("kafka_topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp"),
)



#checkpoint and writing the streaming data to zomato_bronze.raw_events table
checkpoint_path = "/Volumes/my_data/default/checkpoints/zomato_bronze"

query = (
    bronze_stream.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("zomato_bronze.raw_events")
)
# query.awaitTermination()



spark.read.table("zomato_bronze.raw_events").select("kafka_value")\
    .filter("kafka_topic = 'orders'").show(truncate=False)



display(
    spark.read.table("zomato_bronze.raw_events")
    .groupBy("kafka_topic")
    .count()
    .orderBy("kafka_topic")
)



display(
    spark.read.table("zomato_bronze.raw_events")
    .select("kafka_topic", "kafka_value", "kafka_partition", "kafka_offset")
    .limit(5)
)



spark.read.table("zomato_bronze.raw_events").groupBy("kafka_topic").count().show()
