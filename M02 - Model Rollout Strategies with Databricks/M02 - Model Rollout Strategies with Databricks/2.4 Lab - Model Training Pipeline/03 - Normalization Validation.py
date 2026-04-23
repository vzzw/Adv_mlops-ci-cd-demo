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
# MAGIC ## Workflow Notebook - Normalization Validation
# MAGIC
# MAGIC 1. **Purpose of the Notebook**:
# MAGIC    - In this notebook, called **Features Validation**, we will validate the feature table created in the previous notebook.
# MAGIC
# MAGIC 2. **Validation Process**:
# MAGIC    - The feature table is read from the **Feature Store**.
# MAGIC    - The focus is on testing whether **normalization** has been correctly applied to the **normalized column** (e.g., the Age column).
# MAGIC
# MAGIC 3. **Expected Outcome**:
# MAGIC    - If normalization has occurred properly, you will receive a confirmation message indicating that the column has been correctly normalized.

# COMMAND ----------

catalog = dbutils.widgets.get(<FILL_IN>)
schema = dbutils.widgets.get(<FILL_IN>)
normalized_column = dbutils.widgets.get(<FILL_IN>)
silver_table_name = dbutils.widgets.get(<FILL_IN>)

# COMMAND ----------

# MAGIC %skip
# MAGIC catalog = dbutils.widgets.get('catalog')
# MAGIC schema = dbutils.widgets.get('schema')
# MAGIC normalized_column = dbutils.widgets.get('normalized_column')
# MAGIC silver_table_name = dbutils.widgets.get('silver_table_name')

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

# Instantiate the FeatureEngineeringClient
fe = FeatureEngineeringClient()

spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

import numpy as np

# Test function to check normalization
def test_column_normalized(df, column):
    if column not in df.columns:
        raise <FILL_IN>(f"Column '{column}' does not exist in the DataFrame.")
    
    mean = <FILL_IN>
    std = <FILL_IN>
    
    # Allowing a small tolerance for floating-point arithmetic
    tolerance = 1e-4
    <FILL_IN> abs(mean) < tolerance, f"Mean of column '{column}' is not approximately 0. It is {mean}."
    <FILL_IN> abs(std - 1) < tolerance, f"Standard deviation of column '{column}' is not approximately 1. It is {std}."
    print(f"Column '{column}' is properly normalized.")

# COMMAND ----------

# MAGIC %skip
# MAGIC import numpy as np
# MAGIC
# MAGIC # Test function to check normalization
# MAGIC def test_column_normalized(df, column):
# MAGIC     if column not in df.columns:
# MAGIC         raise AssertionError(f"Column '{column}' does not exist in the DataFrame.")
# MAGIC     
# MAGIC     mean = np.mean(df[column])
# MAGIC     std = np.std(df[column])
# MAGIC     
# MAGIC     # Allowing a small tolerance for floating-point arithmetic
# MAGIC     tolerance = 1e-4
# MAGIC     assert abs(mean) < tolerance, f"Mean of column '{column}' is not approximately 0. It is {mean}."
# MAGIC     assert abs(std - 1) < tolerance, f"Standard deviation of column '{column}' is not approximately 1. It is {std}."
# MAGIC     print(f"Column '{column}' is properly normalized.")

# COMMAND ----------

# read table from feature store
df2 = <FILL_IN>

# COMMAND ----------

# MAGIC %skip
# MAGIC # read table from feature store
# MAGIC df2 = fe.read_table(name=f'{silver_table_name}_features').toPandas()

# COMMAND ----------

test_column_normalized(<FILL_IN>)

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC test_column_normalized(df2, f'{normalized_column}_normalized')

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>