from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, when, regexp_extract, col, hour, from_utc_timestamp, lag, unix_timestamp
from pyspark.sql.window import Window
import datetime

spark = SparkSession.builder \
    .appName("Read GCS JSON to DataFrame") \
    .config("spark.jars", "gcs-connector-hadoop3-2.2.2-shaded.jar, spark-3.4-bigquery-0.41.1.jar") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "potent-app-439210-c8-e4406fdd0d2c.json") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .getOrCreate()

today_date = datetime.datetime.now().strftime('%Y-%m-%d')
df = spark.read.json(f"gs://calls_detector_bucket_eu/calls_{today_date}_*")
df = df.withColumn("start_time", from_unixtime(df["start_time"]))
df = df.withColumn(
    "call_duration_category",
    when(df["call_duration"] <= 300, "Short")
    .when(df["call_duration"] <= 600, "Medium")
    .when(df["call_duration"] <= 1200, "Long")
    .otherwise("Very Long")
)
df = df.withColumn("source_area_code", regexp_extract(df['source_no'], r"(\d{3})", 1))
df.show()


#call_counts_df = df.groupBy("call_id").count()
window_spec = Window.partitionBy("source_no").orderBy("start_time")
df_with_lag = df.withColumn("previous_start_time", lag("start_time").over(window_spec))
df_with_time_diff = df_with_lag.withColumn("time_diff", (unix_timestamp(df_with_lag["start_time"]) - unix_timestamp(df_with_lag["previous_start_time"])))

df_with_repeated_flag = df_with_time_diff.withColumn("is_repeated", 
                                                    when((col("time_diff")<= 7200) & (col("previous_start_time").isNotNull()), "yes").otherwise("no")) \
                                                    .drop("previous_start_time", "time_diff")


# df_with_repeated_flag = df.join(call_counts_df, on="call_id", how="left") \
#                         .withColumn("is_repeated", when(col("count") > 1, "yes").otherwise("no")) \
#                         .drop("count")

df_final = df_with_repeated_flag.withColumn("hour_of_day", hour(from_utc_timestamp(df_with_repeated_flag["start_time"], 'Europe/Berlin'))) \
            .withColumn("is_odd_hour", when((col("hour_of_day")>=2) & (col("hour_of_day")<=4), "yes").otherwise("no"))


df_final.show()

project_id = "potent-app-439210-c8"
dataset_id = "scam_call_detector"
table_name = "calls"

df_final.write.format('bigquery') \
    .option('temporaryGcsBucket', 'calls_detector_bucket_eu') \
    .option('project', project_id) \
    .option('dataset', dataset_id) \
    .option('table', table_name) \
    .mode('append') \
    .save()



