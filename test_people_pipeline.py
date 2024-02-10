# Databricks notebook source
def test_gold_output():
    assert spark.table("gold_people"), "Table Not Found"

# COMMAND ----------

def test_columns():
    cols_to_check = ['gender','total_salary','gender_count']
    assert spark.table("gold_people").columns == cols_to_check, "Column not matched"

    

# COMMAND ----------

from pyspark.sql.functions import col

def test_nulls_in_gold():
    df = spark.table("gold_people")
    for column in ['gender','total_salary','gender_count']:
        null_count = df.filter(col(column).isNull()).count()
        assert null_count == 0, f"Column {column} contains {null_count} null values"

# COMMAND ----------

test_gold_output()

# COMMAND ----------

test_columns()

# COMMAND ----------

test_nulls_in_gold()

# COMMAND ----------



# COMMAND ----------


