# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled",True)

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/customers_parquet"))

# COMMAND ----------

df_cust = spark.read.format("parquet")\
    .load("/FileStore/customers_parquet")
df_trans = spark.read.format("parquet")\
    .load("/FileStore/transactions_parquet")

# COMMAND ----------

import time

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joined without AQE and Broadcast

# COMMAND ----------

joined_df = (
    df_trans.join(df_cust, on="cust_id", how="inner")
)
start_time = time.time()
display(joined_df)
print(f"time taken {time.time() - start_time}")

# COMMAND ----------

joined_df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joined using AQE

# COMMAND ----------

joined_aqe_df = (
    df_trans.join(df_cust, on="cust_id", how="inner")
)
start_time = time.time()
display(joined_aqe_df)
print(f"time taken {time.time() - start_time}")

# COMMAND ----------

joined_aqe_df.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Broadcast join

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)

# COMMAND ----------

joined_broadcast_df = (
    df_trans.join(df_cust, on="cust_id", how="inner")
)
start_time = time.time()
display(joined_broadcast_df)
print(f"time taken {time.time() - start_time}")

# COMMAND ----------

joined_broadcast_df.explain()

# COMMAND ----------

