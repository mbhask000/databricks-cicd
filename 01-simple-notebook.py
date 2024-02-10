# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Simple notebook non CI/CD friendly

# COMMAND ----------

bronze_df = spark.read.format("delta").load("dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta")
bronze_df.show(4,truncate=False)

# COMMAND ----------

silver_df = bronze_df.dropDuplicates()
silver_df.show(4,truncate=False)

# COMMAND ----------

for col in silver_df.columns:
    silver_df = silver_df.withColumnRenamed(col, col.replace(" ", "_").lower())
silver_df.show(4,truncate=False)

# COMMAND ----------

from pyspark.sql.functions import col,date_format,mask

cols_changes={"birthdate":date_format(col("birthdate"),"yyyy-MM-dd"), "ssn":mask(col("ssn"))}
silver_df1 = silver_df.withColumns(cols_changes)
silver_df1.show(4,truncate=False)

# COMMAND ----------

from pyspark.sql.functions import sum,count

gold_df = (silver_df.groupBy("gender")
                    .agg(
                        sum("salary").alias("total_salary")
                        ,count("id").alias("gender_count")
                        )
)
gold_df.show(4,truncate=False)

# COMMAND ----------


