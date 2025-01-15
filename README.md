# Scam_call_detector

Project Overview

This project is a real-time data pipeline designed to simulate and analyze active call data, detecting potential scam calls using machine learning algorithms. The pipeline consists of several stages:

Call Data Generation: Random call data is generated and sent to a Kafka topic using a Kafka Producer. I tried to stimulate realistic data as much as I could, considering potential characteristics of a scam call.
Data Storage: The incoming call data is consumed from the Kafka topic and stored in Google Cloud Storage (GCS).
Data Transformation: Data is read from GCS, transformed, and loaded into Google BigQuery for analysis.
Scam Call Detection: A K-Nearest Neighbors (KNN) algorithm is applied to detect scam calls based on specific call features.
Prediction: The trained model is used to predict new scam calls in real time.
The project's architecture includes the integration of several technologies:

Kafka: To stream the call data.
Google Cloud Storage (GCS): For storing incoming data.
BigQuery: For transformed data storage and analytics.
PySpark: For data processing and machine learning.
KNN Classifier: To detect scam calls based on features like call duration, source location, and time of call.
Challenges and Learnings

During the implementation of this project, I realized that integrating Apache Airflow with Kafka for streaming data is not a smooth combination. Kafka, being a real-time streaming service, doesn't align well with Airflow's batch processing model, making it less effective for managing streaming data in real time. This led me to reconsider the usage of Airflow in the pipeline, opting to focus on other tools for orchestration that better fit Kafka's streaming nature.

Technologies Used:

Kafka for real-time data streaming
Google Cloud Storage (GCS) for storing data
BigQuery for data warehousing and analytics
PySpark for data processing
K-Nearest Neighbors (KNN) for machine learning model
