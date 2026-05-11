# Databricks notebook source
from pyspark.sql.functions import avg,col,count,hour, round,second

# COMMAND ----------

hourly_metrics=spark.read.table("zomato_silver.orders")\
    .groupBy(hour(col("timestamp")).alias("hour"),\
        col("city")
        )\
            .agg(count("order_id").alias("order_count"),\
                round(avg("amount"),2).alias("avg_amount"))\
                    .orderBy("hour","city")
hourly_metrics.write.format("delta").mode("overwrite").saveAsTable("zomato_gold.hourly_order_metrics")


# COMMAND ----------

display(spark.read.table("zomato_gold.hourly_order_metrics"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select o.restaurant_id,
# MAGIC count(o.order_id) as order_count,
# MAGIC avg(r.rating) as avg_rating 
# MAGIC from zomato_silver.orders o
# MAGIC left join zomato_silver.ratings r on o.restaurant_id = r.restaurant_id
# MAGIC group by o.restaurant_id
# MAGIC

# COMMAND ----------

del_data= spark.sql("""select order_id,timestamp from zomato_silver.deliveries order by order_id""")
del_data.show(truncate=False)

# COMMAND ----------

delivery_funnel = spark.sql("""
    SELECT 
         avg((unix_timestamp(d2.timestamp) - unix_timestamp(d1.timestamp)) ) as delivery_mins,
         COUNT(d1.order_id) as completed_deliveries
    FROM zomato_silver.orders o
    join zomato_silver.deliveries d1 on o.order_id=d1.order_id
    JOIN zomato_silver.deliveries d2 
        ON d1.order_id = d2.order_id
        AND d1.event_type = 'driver_assigned'
        AND d2.event_type = 'order_delivered'
        group by o.city
    
""")
delivery_funnel.show(truncate=False)