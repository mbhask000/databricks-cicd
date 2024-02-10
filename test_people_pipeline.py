# Databricks notebook source
def test_gold_output():
    assert spark.table("hive_metastore.default.gold_people") is True, "Table Not Found"

# COMMAND ----------

def test_columns():
    cols_to_check = ['gender','total_salary','total_count']
    assert spark.table("hive_metastore.default.gold_people").columns == cols_to_check

    

# COMMAND ----------

from pyspark.sql.functions import col

def test_nulls_in_gold():
    df = spark.table("hive_metastore.default.gold_people")
    for column in ['gender','total_salary','total_count']:
        null_count = df.filter(col(column).isNull()).count()
        assert null_count == 0, f"Column {column} contains {null_count} null values"

# COMMAND ----------

test_gold_output()

# COMMAND ----------

test_columns()

# COMMAND ----------

test_nulls_in_gold()
