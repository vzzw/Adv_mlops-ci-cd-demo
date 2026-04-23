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
# MAGIC ## Workflow Notebook - Train Model on Validated Features
# MAGIC
# MAGIC 1. **Purpose of the Notebook**:
# MAGIC    - In this third notebook, called **Train Model on Features**, we will train a machine learning model using the **validated features** from the previous notebook.
# MAGIC
# MAGIC 2. **Process**:
# MAGIC    - The validated feature table is read and used as input for model training.
# MAGIC    - The resulting model is stored in **Unity Catalog** for centralized management and accessibility.
# MAGIC

# COMMAND ----------

catalog = dbutils.widgets.get(<FILL_IN>)
schema = dbutils.widgets.get(<FILL_IN>)
primary_key = dbutils.widgets.get(<FILL_IN>)
target_column = dbutils.widgets.get(<FILL_IN>)
silver_table_name = dbutils.widgets.get(<FILL_IN>)

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC catalog = dbutils.widgets.get('catalog')
# MAGIC schema = dbutils.widgets.get('schema')
# MAGIC primary_key = dbutils.widgets.get('primary_key')
# MAGIC target_column = dbutils.widgets.get('target_column')
# MAGIC silver_table_name = dbutils.widgets.get('silver_table_name')
# MAGIC delete_column = dbutils.widgets.get('delete_column')

# COMMAND ----------

spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

import mlflow
from mlflow.models.signature import infer_signature
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

# Load dataset
df = spark.read.format('delta').table(silver_table_name)
training_df = df.toPandas()

included_features_list = [<FILL_IN>]
smaller_included_features_list = [<FILL_IN>]

# Split the data into train and test sets
X_large = training_df[included_features_list]
X_small = training_df[smaller_included_features_list]
y = training_df[target_column]
X_large_train, X_large_test, y_large_train, y_large_test = <FILL_IN>
X_small_train = X_large_train.drop(<FILL_IN>)
X_small_test = X_large_test.drop(<FILL_IN>)
y_small_train = y_large_train
y_small_test = y_large_test

# Save the test set for querying later
df = spark.createDataFrame(X_large_test)
df.write.format('delta').mode('overwrite').table(<FILL_IN>)

# COMMAND ----------

# MAGIC %skip
# MAGIC import mlflow
# MAGIC from mlflow.models.signature import infer_signature
# MAGIC from sklearn.ensemble import RandomForestClassifier
# MAGIC from sklearn.model_selection import train_test_split
# MAGIC
# MAGIC # Load dataset
# MAGIC df = spark.read.format('delta').table(silver_table_name)
# MAGIC training_df = df.toPandas()
# MAGIC
# MAGIC included_features_list = [c for c in df.columns if c not in [target_column, primary_key]]
# MAGIC smaller_included_features_list = [c for c in included_features_list if c not in [delete_column]]
# MAGIC
# MAGIC # Split the data into train and test sets
# MAGIC X_large = training_df[included_features_list]
# MAGIC X_small = training_df[smaller_included_features_list]
# MAGIC y = training_df[target_column]
# MAGIC X_large_train, X_large_test, y_large_train, y_large_test = train_test_split(X_large, y, test_size=0.2, random_state=42)
# MAGIC X_small_train = X_large_train.drop(columns=[delete_column])
# MAGIC X_small_test = X_large_test.drop(columns=[delete_column])
# MAGIC y_small_train = y_large_train
# MAGIC y_small_test = y_large_test
# MAGIC
# MAGIC df = spark.createDataFrame(X_large_test)
# MAGIC df.write.format('delta').mode('overwrite').saveAsTable('X_large_test')

# COMMAND ----------

from mlflow.tracking import MlflowClient
def train_model(X_train,y, alias):
    # Start MLflow run
    with mlflow.start_run(run_name='mlflow-run') as run:
        # Initialize the Random Forest classifier
        rf_classifier = RandomForestClassifier(random_state=42)

        # Fit the model on the training data
        rf_classifier.fit(X_train, y_large_train)

        # Enable autologging
        <FILL_IN>

        # Define the registered model name
        registered_model_name = f"{catalog}.{schema}.my_model_{schema}"

        mlflow.sklearn.log_model(
        <FILL_IN>
        )

        model_uri = f"runs:/{run.info.run_id}/model-artifacts"

    mlflow.set_registry_uri("databricks-uc")

    # Define the model name 
    model_name = f"{catalog}.{schema}.my_model_{schema}"

    # Register the model in the model registry
    registered_model = <FILL_IN>

    # Initialize an MLflow Client
    client = MlflowClient()

    # Assign an alias
    client.set_registered_model_alias(
        name= <FILL_IN>,  # The registered model name
        alias=<FILL_IN>,  # The alias representing the dev environment
        version=<FILL_IN>  # The version of the model you want to move to "dev"
    )

train_model(X_large_train,y_large_train, 'a')
train_model(X_small_train,y_small_train, 'b')

# COMMAND ----------

# MAGIC %skip
# MAGIC import mlflow
# MAGIC from mlflow.tracking import MlflowClient
# MAGIC def train_model(X_train,y, alias):
# MAGIC     # Start MLflow run
# MAGIC     with mlflow.start_run(run_name='mlflow-run') as run:
# MAGIC         # Initialize the Random Forest classifier
# MAGIC         rf_classifier = RandomForestClassifier(random_state=42)
# MAGIC
# MAGIC         # Fit the model on the training data
# MAGIC         rf_classifier.fit(X_train, y_large_train)
# MAGIC
# MAGIC         # Enable autologging
# MAGIC         mlflow.sklearn.autolog(log_input_examples=True, silent=True)
# MAGIC
# MAGIC         # Define the registered model name
# MAGIC         registered_model_name = f"{catalog}.{schema}.my_model_{schema}"
# MAGIC
# MAGIC         mlflow.sklearn.log_model(
# MAGIC         rf_classifier,
# MAGIC         artifact_path = "model-artifacts", 
# MAGIC         input_example=X_train[:3],
# MAGIC         signature=infer_signature(X_train, y_large_train)
# MAGIC         )
# MAGIC
# MAGIC         model_uri = f"runs:/{run.info.run_id}/model-artifacts"
# MAGIC
# MAGIC     mlflow.set_registry_uri("databricks-uc")
# MAGIC
# MAGIC     # Define the model name 
# MAGIC     model_name = f"{catalog}.{schema}.my_model_{schema}"
# MAGIC
# MAGIC     # Register the model in the model registry
# MAGIC     registered_model = mlflow.register_model(model_uri=model_uri, name=model_name)
# MAGIC
# MAGIC     # Initialize an MLflow Client
# MAGIC     client = MlflowClient()
# MAGIC
# MAGIC     # Assign an alias
# MAGIC     client.set_registered_model_alias(
# MAGIC         name= registered_model.name,  # The registered model name
# MAGIC         alias=alias,  # The alias representing the dev environment
# MAGIC         version=registered_model.version  # The version of the model you want to move to "dev"
# MAGIC     )
# MAGIC
# MAGIC train_model(X_large_train,y_large_train, 'a')
# MAGIC train_model(X_small_train,y_small_train, 'b')

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>