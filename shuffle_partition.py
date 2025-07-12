# Databricks notebook source
from pyspark.sql.functions import *
import time

# COMMAND ----------

df = spark.read.format('parquet')\
    .load("/FileStore/transactions_parquet")

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# spark.conf.set("spark.sql.adaptive.enabled",False)
# spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled",False)
# spark.conf.set("spark.sql.adaptive.skewJoin.enabled", False)
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

df1 =df.groupBy(col('cust_id')).agg(count(col('txn_id')).alias('count')).orderBy(desc('count')).collect()

start_time = time.time()
display(df1)
print(f"time taken {time.time() - start_time}")

# COMMAND ----------

df = spark.read.format('csv')\
    .option("inferSchema", True)\
    .option("header",True)\
    .load("/FileStore/tables/zomato.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

df_city = df.filter(col('City')=="Pasay City")

start_time = time.time()
display(df_city)
print(f"time taken {time.time() - start_time}")

# COMMAND ----------

city_df = df.write.mode("overwrite").partitionBy("City").parquet("/FileStore/tables/repartitioned_city")

# COMMAND ----------

df1 = spark.read.format('parquet')\
    .load("/FileStore/tables/repartitioned_city")


# COMMAND ----------

df_city = df1.filter(col('City')=="Pasay City")

start_time = time.time()
display(df_city)
print(f"time taken {time.time() - start_time}")

# COMMAND ----------

df.repartition(2).write.mode("overwrite").partitionBy("City").parquet("/FileStore/tables/repartitioned_by2_city")

# COMMAND ----------

df2 = spark.read.format('parquet')\
    .load("/FileStore/tables/repartitioned_by2_city")

# COMMAND ----------

df_city = df2.filter(col('City')=="Pasay City")

start_time = time.time()
display(df_city)
print(f"time taken {time.time() - start_time}")

# COMMAND ----------

df.coalesce(2).write.mode("overwrite").parquet("/FileStore/tables/coalesce_by10")

# COMMAND ----------

