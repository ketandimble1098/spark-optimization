# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 3)
spark.conf.set("spark.sql.adaptive.enabled", False)
spark.conf.get("spark.sql.shuffle.partitions")


# COMMAND ----------

# MAGIC %md
# MAGIC Simulating skewed join

# COMMAND ----------

df_uniform = spark.createDataFrame([i for i in range(1000000)], IntegerType())
df_uniform.show()

# COMMAND ----------

df_uniform.withColumn("partition", spark_partition_id()).groupBy("partition").count().orderBy("partition").show()

# COMMAND ----------

df0 = spark.createDataFrame([0]*999990,IntegerType()).repartition(1)
df1 = spark.createDataFrame([1]*15, IntegerType()).repartition(1)
df2 = spark.createDataFrame([2]*10, IntegerType()).repartition(1)
df3 = spark.createDataFrame([3]*5, IntegerType()).repartition(1)
df_skewed = df0.union(df1).union(df2).union(df3)
df_skewed.show()

# COMMAND ----------

df_skewed.withColumn("partition", spark_partition_id()).groupBy("partition").count().orderBy("partition").show()

# COMMAND ----------

df_join = df_skewed.join(df_uniform,"value","inner")

# COMMAND ----------

df_join.withColumn("partition", spark_partition_id()).groupBy("partition").count().orderBy("partition").show()

# COMMAND ----------

salt_number = int(spark.conf.get("spark.sql.shuffle.partitions"))
salt_number

# COMMAND ----------

# MAGIC %md
# MAGIC Assign random number between shuffle partions

# COMMAND ----------

df_skewed_1 = df_skewed.withColumn("salt",(rand()* salt_number).cast("int"))

# COMMAND ----------

df_skewed_1.show(truncate=False)

# COMMAND ----------

df_uniform_1 = df_uniform.withColumn("salt_values", array([lit(i) for i in range(salt_number)])).withColumn("salt", explode(col("salt_values")))

# COMMAND ----------

df_uniform_1.show()

# COMMAND ----------

df_join_1 = df_skewed_1.join(df_uniform_1,["value","salt"], "inner")

# COMMAND ----------

df_join_1.withColumn("partition", spark_partition_id()).groupBy("value","partition").count().orderBy("value","partition").show()

# COMMAND ----------

# MAGIC %md
# MAGIC salting in grouby

# COMMAND ----------

df_skewed.groupBy("value").count().show()

# COMMAND ----------

df_skewed.withColumn("salt",(rand()* salt_number).cast("int")).groupBy("value","salt").agg(count("value").alias("count")).groupBy("value").agg(sum("count").alias("count")).show()

# COMMAND ----------

