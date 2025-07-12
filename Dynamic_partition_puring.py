# Databricks notebook source
# MAGIC %md
# MAGIC - Pruning partitions at runtime
# MAGIC - Analyse the listening activity of users on the release date of a song on/after 2020-01-01

# COMMAND ----------

from pyspark.storagelevel import StorageLevel
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

listening_actv_pt = spark.read.format('csv').option("inferSchema",True).option("header",True).load("dbfs:/FileStore/tables/Spotify_Listening_Activity.csv")

# COMMAND ----------

df_listening_actv = (
    listening_actv_pt
    .withColumnRenamed("listen_date", "listen_time")
    .withColumn("listen_date", to_date("listen_time", "yyyy-MM-dd HH:mm:ss.SSSSSS"))
)

# COMMAND ----------

df_listening_actv.show()

# COMMAND ----------

df_listening_actv.write.mode("overwrite").partitionBy("listen_date").parquet("/FileStore/tables/partitionby_listen_date")

# COMMAND ----------

df_listening = spark.read.parquet("/FileStore/tables/partitionby_listen_date")
df_listening.show(5, False)

# COMMAND ----------

df_songs = spark.read.csv("/FileStore/tables/Spotify_Songs.csv", header=True, inferSchema=True)
df_songs.printSchema()

# COMMAND ----------

df_songs = (
    df_songs
    .withColumnRenamed("release_date", "release_datetime")
    .withColumn("release_date", to_date("release_datetime", "yyyy-MM-dd HH:mm:ss.SSSSSS"))
)
df_songs.show(5, False)
df_songs.printSchema()

# COMMAND ----------

df_selected_songs = df_songs.filter(col("release_date") > lit("2019-12-31"))


df_listening_actv_of_selected_songs = df_listening.join(
    df_selected_songs, 
    on=(df_songs.release_date == df_listening.listen_date) & (df_songs.song_id == df_listening.song_id), 
    how="inner"
)

# COMMAND ----------

df_listening_actv_of_selected_songs.explain(True)

# COMMAND ----------

df_listening_actv_of_selected_songs.show()

# COMMAND ----------

