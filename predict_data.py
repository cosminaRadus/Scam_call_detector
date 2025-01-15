from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.pipeline import PipelineModel
from pyspark.sql.functions  import when, col, udf, rand, lit, monotonically_increasing_id, row_number
from pyspark.sql.window import Window
import joblib
import numpy as np
import pandas as pd


spark = SparkSession.builder \
    .appName("Predict new calls") \
    .config("spark.jars", "gcs-connector-hadoop3-2.2.2-shaded.jar, spark-3.4-bigquery-0.41.1.jar") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "potent-app-439210-c8-e4406fdd0d2c.json") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .getOrCreate()

bigquery_table = "potent-app-439210-c8.scam_call_detector.calls"

df = spark.read.format("bigquery") \
    .option("table", bigquery_table) \
    .load()

df_new_transformed = df.withColumn(
    "is_repeated_numeric", when(col("is_repeated") == "yes", 1).otherwise(0)
).withColumn(
    "is_odd_hour_numeric", when(col("is_odd_hour") == "yes", 1).otherwise(0)
)

pipeline_model = PipelineModel.load("pipeline_model")
knn_loaded = joblib.load('knn_model.pkl')

df_new_transformed = pipeline_model.transform(df_new_transformed)
df_new_transformed.show()

pandas_new_df = df_new_transformed.select("scaledFeatures").toPandas()
X_new = np.array([row['scaledFeatures'].toArray() for index, row in pandas_new_df.iterrows()])

y_new_pred = knn_loaded.predict(X_new)

print(f"ALO new predictions: {y_new_pred[:70]}")

df_new_transformed = df_new_transformed.withColumn(
    "scam_flag", 
    lit(None).cast("int")  # Placeholder for predictions
)

# Map predictions to new rows by creating a new column using monotonically_increasing_id
window_df = Window.orderBy(lit(1))
df_with_predictions = df_new_transformed.withColumn(
    "prediction_index", row_number().over(window_df)
)

#df_with_predictions.show()

# Create a DataFrame from the predictions and map the index to the DataFrame rows
predictions_df = pd.DataFrame({"scam_flag": y_new_pred})
predictions_spark_df = spark.createDataFrame(predictions_df)

window_pred = Window.orderBy(lit(1))  # Order by a constant to ensure row_number is applied sequentially

predictions_spark_df = predictions_spark_df.withColumn("prediction_index", row_number().over(window_pred))


#predictions_spark_df.show()

# Join the predictions with the original DataFrame
df_new_transformed_with_preds = df_with_predictions.join(
    predictions_spark_df,
    df_with_predictions["prediction_index"] == predictions_spark_df["prediction_index"],
    "left_outer"
).drop(df_with_predictions["scam_flag"])

df_new_transformed_with_preds = df_new_transformed_with_preds.select("call_id", "call_duration", "source_location", "source_no",  "call_duration_category", "is_repeated", "hour_of_day", "is_odd_hour", predictions_spark_df.scam_flag.alias("scam_flag"))


# Show the final DataFrame with predictions
#df_new_transformed_with_preds.show()

project_id = "potent-app-439210-c8"
dataset_id = "scam_call_detector"
table_name = "calls_predicted"

df_new_transformed_with_preds.write.format('bigquery') \
    .option('temporaryGcsBucket', 'calls_detector_bucket_eu') \
    .option('project', project_id) \
    .option('dataset', dataset_id) \
    .option('table', table_name) \
    .mode('append') \
    .save()