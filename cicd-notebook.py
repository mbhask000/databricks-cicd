# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Simple CI/CD friendly

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------


class people_demo:
    def __init__(self, url):
        self.data_url = url

    def load_bronze(self):
        bronze_df = spark.read.format("delta").load(self.data_url)
        return bronze_df

    def generate_silver(self, bronze_df):
        silver_df = bronze_df.dropDuplicates()
        for col in silver_df.columns:
            silver_df = silver_df.withColumnRenamed(col, col.replace(" ", "_").lower())

        cols_changes = {
            "birthdate": F.date_format(F.col("birthdate"), "yyyy-MM-dd"),
            "ssn": F.mask(F.col("ssn"))
        }
        silver_df1 = silver_df.withColumns(cols_changes)
        return silver_df1

    def process_gold(self, silver_df):
        gold_df = silver_df.groupBy("gender").agg(
            F.sum("salary").alias("total_salary"), F.count("id").alias("gender_count")
        )

        return gold_df

# COMMAND ----------

if __name__ == "__main__":
    obj_people = people_demo("dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta")

    bronze_df = obj_people.load_bronze()
    silver_df = obj_people.generate_silver(bronze_df)
    gold_df = obj_people.process_gold(silver_df)

    gold_df.write.mode('overwrite').saveAsTable("hive_metastore.default.gold_people")

    # bronze_df.show(4,truncate=False)
    # bronze_df.printSchema()
    # silver_df.show(4,truncate=False)
    # gold_df.show(4,truncate=False)

    ## Don't forget to comment the show functions for Unit Testing

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from gold_people

# COMMAND ----------


