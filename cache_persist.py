# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

cust_df = spark.read.parquet("/FileStore/customers_parquet")
display(cust_df)

# COMMAND ----------

df_base = (
    cust_df.filter(col("city") == "boston")
    .withColumn("cust_group",
                when(
                    col("age").between(20, 30),
                    lit("young")
                ).when(
                    col("age").between(31, 50),
                    lit("mid")                   
                ).when(
                    col("age") > 50,
                    lit("old")
                ).otherwise(lit('kid'))
                ).select("cust_id", "name", "gender", "birthday", "zip", "city","cust_group")
)
df_base.cache()

# COMMAND ----------

df_base.show()

# COMMAND ----------

# MAGIC %md
# MAGIC without cache

# COMMAND ----------

df1 = (
    df_base.withColumn("test_col", lit("test"))
    .withColumn("birth_yr", split("birthday","/").getItem(2))
)
df1.explain(True)
df1.show()

# COMMAND ----------

df2 = (
    df_base.withColumn("test_col", lit("test_month"))
    .withColumn("birth_month", split("birthday","/").getItem(1))
)
df2.explain(True)
df2.show()

# COMMAND ----------

df3 = (
    df_base.withColumn("test_col", lit("test"))
    .withColumn("birth_yr", split("birthday","/").getItem(2))
)
df3.explain(True)
df3.show()

# COMMAND ----------

df4 = (
    df_base.withColumn("test_col", lit("test_month"))
    .withColumn("birth_month", split("birthday","/").getItem(1))
)
df4.explain(True)
df4.show()

# COMMAND ----------

df_base.unpersist()

# COMMAND ----------

from pyspark.storagelevel import *

# COMMAND ----------

df_base.persist(StorageLevel.MEMORY_ONLY)
df4 = (
    df_base.withColumn("test_col", lit("test_month"))
    .withColumn("birth_month", split("birthday","/").getItem(1))
)
df4.explain(True)
df4.show()

# COMMAND ----------

df_base.persist(StorageLevel.MEMORY_AND_DISK_DESER)
df4 = (
    df_base.withColumn("test_col", lit("test_month"))
    .withColumn("birth_month", split("birthday","/").getItem(1))
)
df4.explain(True)
df4.show()

# COMMAND ----------

