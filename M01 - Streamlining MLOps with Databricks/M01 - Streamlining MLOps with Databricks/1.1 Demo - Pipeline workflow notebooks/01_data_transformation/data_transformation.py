# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Transformation
# MAGIC This notebook performs feature engineering on the cleaned dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run this notebook, you need a classic cluster running one of the following Databricks runtime(s): **17.3.x-cpu-ml-scala2.13**. **Do NOT use serverless compute to run this notebook**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Before starting the demo, run the provided classroom setup script.

# COMMAND ----------

# MAGIC %run ../../../Includes/Classroom-Setup

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import col, log, sqrt
# Read the cleaned data from Delta format
cleaned_data = spark.table(f"{DA.catalog_name}.{DA.schema_name}.cleaned_diabetes_data")

# Display the cleaned data
display(cleaned_data)

# COMMAND ----------

# Apply feature engineering (example transformations)
transformed_data = cleaned_data \
    .withColumn("log_BMI", log(col("BMI") + 1)) \
    .withColumn("sqrt_PhysHlth", sqrt(col("PhysHlth") + 1)) \
    .withColumn("log_Age", log(col("Age") + 1)) \
    .withColumn("sqrt_MentHlth", sqrt(col("MentHlth") + 1))

# Drop unnecessary columns
columns_to_drop = ["HighChol", "HvyAlcoholConsump"]  # Example columns to drop; adjust as needed
transformed_data = transformed_data.drop(*columns_to_drop)

# Display the transformed data
display(transformed_data)

# COMMAND ----------

# Define the path to save the transformed data
transformed_data_path = f"{DA.catalog_name}.{DA.schema_name}.transformed_diabetes_data"

# Write the transformed DataFrame to Delta format
transformed_data.write.format("delta").mode("overwrite").saveAsTable(transformed_data_path)

print(f"Transformed data saved to: {transformed_data_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>