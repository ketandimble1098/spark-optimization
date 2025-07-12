# Databricks notebook source
# MAGIC %md
# MAGIC Divide the dataset into more managable buckets

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

df_order = spark.read.format("csv").option("header",True).option("inferSchema", True).load("dbfs:/FileStore/tables/orders.csv")

# COMMAND ----------

df_product = spark.read.format("csv").option("header",True).option("inferSchema", True).load("dbfs:/FileStore/tables/products.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Bucketing in joins

# COMMAND ----------

joined_df = (
    df_order.join(
        df_product,
        on="product_id",
        how="inner"
    )
)

# COMMAND ----------

joined_df.explain()

# COMMAND ----------

df_product.write.bucketBy(4,col="product_id").mode("overwrite").saveAsTable("product_bucketed")

# COMMAND ----------

