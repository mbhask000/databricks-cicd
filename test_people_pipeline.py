# Databricks notebook source
from cicd-notebook import people_demo

# COMMAND ----------

def test_bronze():
    url = "dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta"
    obj_people = people_demo(url)
    
    bronze_df = obj_people.load_bronze()
    assert bronze_df.count() > 0, "Not greater than zero"

# COMMAND ----------

def test_silver():
    url = "dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta"
    obj_people = people_demo(url)
    
    bronze_df = obj_people.load_bronze()
    silver_df = obj_people.generate_silver(bronze_df)

    cols_to_check = ['id','firstname','middlename','lastname','gender','birthdate','ssn','salary']
    assert silver_df.columns == cols_to_check

    

# COMMAND ----------

from pyspark.sql.functions import col

def test_nulls_in_silver():
    url = "dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta"
    obj_people = people_demo(url)
    
    bronze_df = obj_people.load_bronze()
    silver_df = obj_people.generate_silver(bronze_df)
    for column in ["firstname", "lastname", "gender","birthdate"]:
        null_count = silver_df.filter(col(column).isNull()).count()
        assert null_count == 0, f"Column {column} contains {null_count} null values"

# COMMAND ----------

#test_bronze()

# COMMAND ----------

#test_silver()

# COMMAND ----------

#test_nulls_in_silver()
