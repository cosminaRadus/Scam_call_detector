from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler 
from pyspark.ml import Pipeline
from pyspark.sql import functions as F
from pyspark.sql.functions  import when, col, udf, rand
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
import math
import pandas as pd
import numpy as np
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import classification_report



spark = SparkSession.builder \
    .appName("BigQuery Integration") \
    .config("spark.jars", "spark-3.4-bigquery-0.41.1.jar") \
    .getOrCreate()

bigquery_table = "potent-app-439210-c8.scam_call_detector.calls"
df = spark.read.format("bigquery") \
    .option("table", bigquery_table) \
    .load()


# LABEL TRAINING/HISTORICAL DATASET

def get_scam_probability(df):

    return df.withColumn("scam_probability",
                        when(
                            (col("is_repeated") == "yes") & (col("is_odd_hour") == "yes"), rand() < 0.9
                        ).when(
                            (col("is_odd_hour") == "yes"), rand() < 0.5 
                        ).when(
                            (col("is_repeated") == "yes"), rand() < 0.3
                        ).otherwise(rand() < 0.15)
    )

df_probabilistic = get_scam_probability(df)

df_labeled = df_probabilistic.withColumn("scam_flag",
                                        when(col("scam_probability") == True, 1).otherwise(0)) \
                                        .drop("scam_probability")

# features = df_labeled.select("call_duration", "hour_of_day", "is_repeated", 
#                              "is_odd_hour", "source_area_code").rdd.map(lambda row: row).collect()

# labels = df_labeled.select("scam_flag").rdd.map(lambda row: row["scam_flag"]).collect()

# X = np.array([list(map(float, feature)) for feature in features])  # Convert features to a numerical array
# y = np.array(labels)  # Labels (scam or not scam)

df_transformed = df_labeled.withColumn(
    "is_repeated_numeric", when(col("is_repeated") == "yes", 1).otherwise(0)
).withColumn(
    "is_odd_hour_numeric", when(col("is_odd_hour") == "yes", 1).otherwise(0)
)

assembler = VectorAssembler(
    inputCols=["call_duration", "hour_of_day", "is_repeated_numeric", "is_odd_hour_numeric"],
    outputCol="features"
)

scaler = StandardScaler(inputCol="features",  
                        outputCol="scaledFeatures",  
                        withStd=True,  
                        withMean=False) 

pipeline = Pipeline(stages=[assembler, scaler])

pipeline_model = pipeline.fit(df_transformed)
df_transformed = pipeline_model.transform(df_transformed)

pandas_df = df_transformed.select("scaledFeatures", "scam_flag").toPandas()

train_data, test_data = df_transformed.randomSplit([0.8, 0.2], seed=42)

X = np.array([row['scaledFeatures'].toArray() for index, row in pandas_df.iterrows()])
y = pandas_df['scam_flag'].values

knn = KNeighborsClassifier(n_neighbors=5)
knn.fit(X, y)

y_pred = knn.predict(X)

print("Classification Report:\n", classification_report(y, y_pred))