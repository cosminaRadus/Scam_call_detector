from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import subprocess
import logging
import time

# Define a simple Python function
def print_hello():
    print("Hello, Airflow!")

# Define the DAG
dag = DAG(
    'simple_dag', 
    description='A simple Airflow DAG',
    schedule_interval='@daily', 
    start_date=datetime(2025, 1, 15),
    catchup=False,  
)

def generate_calls_task():
    logging.info("Running generate_calls.py script")
    result = subprocess.run(["python3", "/Users/kokorad/Desktop/ScamCall/generate_calls.py"], capture_output=True, text=True)
    logging.info(f"generate_calls.py output: {result.stdout}")


generate_calls = PythonOperator(
    task_id='generate_calls',
    python_callable=generate_calls_task,
    dag=dag
)

def store_data_to_gcs_task():
    subprocess.run(["python3", "/Users/kokorad/Desktop/ScamCall/store_data_to_gcs.py"], capture_output=True, text=True)
    #time.sleep(10)

store_data_to_gcs = PythonOperator(
    task_id='store_data_to_gcs',
    python_callable=store_data_to_gcs_task,
    dag=dag
)

def store_data_to_bigquery_task():
    subprocess.run(["python3", "/Users/kokorad/Desktop/ScamCall/store_data_to_bigQuery.py"], capture_output=True, text=True)

store_data_to_bigquery = PythonOperator(
    task_id='store_data_to_bigquery',
    python_callable=store_data_to_bigquery_task,
    dag=dag
)

def train_model_task():
    subprocess.run(["python3", "/Users/kokorad/Desktop/ScamCall/classifier.py"], capture_output=True, text=True)

train_model = PythonOperator(
    task_id='train_model',
    python_callable=train_model_task,
    dag=dag
)

def predict_data_task():
    subprocess.run(["python3", "/Users/kokorad/Desktop/ScamCall/predict_data.py"], capture_output=True, text=True)

predict_data = PythonOperator(
    task_id='predict_data',
    python_callable=predict_data_task,
    dag=dag
)

generate_calls >> store_data_to_gcs >> store_data_to_bigquery >> train_model >> predict_data