# Databricks notebook source
# MAGIC %md
# MAGIC ### Add configuration

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleaning previous data to consistency

# COMMAND ----------

dbutils.fs.rm(raw_path, True)
dbutils.fs.rm(bronze_path, True)
dbutils.fs.rm(silver_path, True)
dbutils.fs.rm(gold_path, True)
dbutils.fs.rm(checkpoint_path, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create stream 

# COMMAND ----------

raw_df = (
  spark.readStream.format("cloudFiles")
  .option("cloudFiles.partitionColumns", "year, month, day")
  .option("cloudFiles.format", "parquet")
  .option("cloudFiles.includeExistingFiles", True)
  .option("cloudFiles.maxFilesPerTrigger", 10)
  .option("cloudFiles.maxBytesPerTrigger", "10m")
  .schema(raw_data_schema)
  .load(raw_data_dir)
)
display(raw_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Bronze stream from raw data stream

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

bronze_stream = (
    raw_df.select(
        "*", 
        lit("bd201stacc/m13sparkstreaming").alias("datasource"),
        current_timestamp().alias("ingesttime")
    )
    .writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronze_checkpoint)
    .partitionBy("wthr_date")
    .queryName("write_raw_to_bronze")
    .start(bronze_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check stream status

# COMMAND ----------

bronze_stream.status

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Delta table

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS hotel_weather_silver")
spark.sql(f"""
CREATE TABLE `hotel_weather_silver` ( 
    `wthr_date` STRING, 
    `country` STRING, 
    `city` STRING, 
    `num_distinct_hotels` BIGINT, 
    `avg_tmpr` DOUBLE, 
    `max_tmpr` DOUBLE, 
    `min_tmpr` DOUBLE) 
    USING delta PARTITIONED BY (wthr_date) 
    LOCATION '{silver_path}'
""")


# COMMAND ----------

from delta.tables import DeltaTable

silver_table = DeltaTable.forName(spark, "hotel_weather_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1. Using Spark calculate in Databricks Notebooks for each city each day:
# MAGIC 
# MAGIC     * Number of distinct hotels in the city.
# MAGIC     * Average/max/min temperature in the city.
# MAGIC 
# MAGIC Note. There is no function Distinct on streaming DF. Need to use approx_count_distinct()

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct, avg, max, min, round

silver_stream_df = (
  spark.readStream
  .format("delta")
  .load(bronze_path)
  .groupBy("wthr_date", "country", "city")
  .agg(
    approx_count_distinct("id", 0.03).alias("num_distinct_hotels"),
    round(avg("avg_tmpr_c"), 1).alias("avg_tmpr"),
    max("avg_tmpr_c").alias("max_tmpr"),
    min("avg_tmpr_c").alias("min_tmpr"))
)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Update silver table by batches

# COMMAND ----------

def upsert_to_delta(micro_batch_df, batch_id):
  (silver_table.alias("t").merge(micro_batch_df.alias("s"), "s.wthr_date = t.wthr_date AND s.country = t.country AND s.city = t.city")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())

(silver_stream_df.writeStream
  .format("delta")
  .foreachBatch(upsert_to_delta)
  .outputMode("update")
  .option("checkpointLocation", silver_checkpoint) 
  .start()
)

# COMMAND ----------

silver_stream_df.explain(True)

# COMMAND ----------

dbutils.fs.ls(silver_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2. Visualize incoming data in Databricks Notebook for 10 biggest cities (the biggest number of hotels in the city, one chart for one city):
# MAGIC 
# MAGIC     * X-axis: date (date of observation).
# MAGIC     * Y-axis: number of distinct hotels, average/max/min temperature.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import *
 
window_func = Window.partitionBy("wthr_date").orderBy(desc("num_distinct_hotels"))

# COMMAND ----------

from pyspark.sql.types import IntegerType

top_ten_biggest_cities = (silver_table.toDF()
  .select("wthr_date", "country", "city", "num_distinct_hotels", "avg_tmpr", "max_tmpr", "min_tmpr", row_number().over(window_func).cast(IntegerType()).alias("rnum"))
  .filter(col("rnum") <= 10)
)

# COMMAND ----------

display(top_ten_biggest_cities)
