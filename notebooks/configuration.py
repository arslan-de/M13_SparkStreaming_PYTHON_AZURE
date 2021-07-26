# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.bd201stacc.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.bd201stacc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.bd201stacc.dfs.core.windows.net", "f3905ff9-16d4-43ac-9011-842b661d556d")
spark.conf.set("fs.azure.account.oauth2.client.secret.bd201stacc.dfs.core.windows.net", "mAwIU~M4~xMYHi4YX_uT8qQ.ta2.LTYZxT")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.bd201stacc.dfs.core.windows.net", "https://login.microsoftonline.com/b41b72d0-4e9f-4c26-8a69-f949f367c91d/oauth2/token")
spark.conf.set("fs.azure.account.key.stm13westeurope.dfs.core.windows.net", "uF8LcOvyEqULgM+DWzEIJPeEMzqyOnJqX6TJLtLDQNzm5xwQEEdk/gs0lbqW+MEs/BT3tuoCzRQkYxet0g0uTw==")

# COMMAND ----------

raw_data_dir = "abfss://m13sparkstreaming@bd201stacc.dfs.core.windows.net/hotel-weather/"

# COMMAND ----------

result_path = "abfss://data@stm13westeurope.dfs.core.windows.net/"

mount_data = "/mnt/data/"
raw_path = mount_data + "raw/"
bronze_path = mount_data + "bronze/"
silver_path = mount_data + "silver/"
gold_path = mount_data + "gold/"

checkpoint_path = mount_data + "checkpoints/"
bronze_checkpoint = checkpoint_path + "bronze/"
silver_checkpoint = checkpoint_path + "silver/"
gold_checkpoint = checkpoint_path + "gold/"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

raw_data_schema = StructType([
    StructField("address", StringType(), True),
    StructField("avg_tmpr_c", DoubleType(), True),
    StructField("avg_tmpr_f", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("geoHash", StringType(), True),
    StructField("id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("name", StringType(), True),
    StructField("wthr_date", StringType(), True),
    StructField("year", StringType(), True),
    StructField("month", StringType(), True),
    StructField("day", StringType(), True)
])

# COMMAND ----------

# Code to check schema
# df = spark.read.format("parquet").schema(schema).load(raw_data_dir)
# df.schema

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Configuration complete
