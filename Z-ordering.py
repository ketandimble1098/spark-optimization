# Databricks notebook source
# dbfs:/databricks-datasets/definitive-guide/data/retail-data/all/online-retail-dataset.csv

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/definitive-guide/data/retail-data/all/online-retail-dataset.csv"))

# COMMAND ----------

df = spark.read.csv(path = "dbfs:/FileStore/tables/sales.csv", inferSchema = True, header=True)
df.repartition(16).write.format("delta").mode("overwrite").option("path","/FileStore/delta_tables/").saveAsTable("sales_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_delta WHERE trx_id = '791687534'

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(trx_id), max(trx_id), _metadata.file_name from sales_delta
# MAGIC group by _metadata.file_name
# MAGIC order by min(trx_id)

# COMMAND ----------

# MAGIC %md
# MAGIC control output file size to 64mb

# COMMAND ----------

spark.conf.set("spark.databricks.delta.optimize.maxFileSize", 524288)

# COMMAND ----------

# MAGIC %md
# MAGIC single column z-ordering

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE sales_delta ZORDER BY (trx_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select min(trx_id), max(trx_id), _metadata.file_name from sales_delta
# MAGIC group by _metadata.file_name
# MAGIC order by min(trx_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_delta WHERE trx_id = '791687534'

# COMMAND ----------

# MAGIC %md
# MAGIC Multicolumn z-ordering

# COMMAND ----------

# MAGIC %sql
# MAGIC select city_id,min(trx_id), max(trx_id), _metadata.file_name from sales_delta
# MAGIC group by city_id, _metadata.file_name
# MAGIC order by min(trx_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE sales_delta ZORDER BY (trx_id,city_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_delta WHERE trx_id = '791687534'

# COMMAND ----------

