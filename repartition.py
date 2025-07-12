# Databricks notebook source
display(dbutils.fs.ls('/FileStore/tables'))

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "false")

# COMMAND ----------

zomato_df = spark.read.format('csv')\
        .option('inferSchema', True)\
        .option('header',True)\
        .load('dbfs:/FileStore/tables/zomato.csv')

# COMMAND ----------

display(zomato_df)

# COMMAND ----------

zomato_df.describe()

# COMMAND ----------

zomato_df.rdd.getNumPartitions()

# COMMAND ----------

zomato_df.repartition('City').explain(True)

# COMMAND ----------

repartitioned_df = zomato_df.repartition(10)

# COMMAND ----------

repartitioned_df.rdd.getNumPartitions()

# COMMAND ----------

repartitioned_df.write.format("parquet")\
    .mode("overwrite")\
    .option("path","/FileStore/tables/repartitioned_by_num")\
    .save()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df = spark.read.format("parquet")\
        .load("/FileStore/tables/repartitioned_by_num")

df_new = df.filter(col("City") ==  "New Delhi")
display(df_new)

# COMMAND ----------

df.write.format("parquet")\
    .mode("append")\
    .partitionBy("City")\
    .option("path","/FileStore/tables/partitionBy_num")\
    .save()

# COMMAND ----------

df = spark.read.format("parquet")\
        .load("/FileStore/tables/partitionBy_num")

df_new = df.filter(col("City") ==  "New Delhi")
display(df_new)

# COMMAND ----------

