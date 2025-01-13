import json
import time 
from kafka import KafkaConsumer
from google.cloud import storage
import datetime

consumer = KafkaConsumer(
    'calls_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest'
)

storage_client = storage.Client()
bucket = storage_client.bucket("calls_bucket_detector")

message_batch = []

def write_batch_to_gsc(batch_data):

    current_time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    blob_name = f"calls_{current_time}"
    blob = bucket.blob(blob_name)
    batch_data_decoded = []
    for msg in batch_data:

        decoded_msg = msg.decode('utf-8')

        if decoded_msg.strip():  # the message isn't empty
            batch_data_decoded.append(json.loads(decoded_msg))

    if batch_data_decoded:
        blob.upload_from_string(json.dumps(batch_data_decoded), content_type='application/json')
        print(f"Uploaded data to GCS: {blob_name}")


flush_interval = 10   # 1 hour
last_flush_time = time.time()

for message in consumer:
    message_batch.append(message.value)

    if (time.time() - last_flush_time) >= flush_interval:
        write_batch_to_gsc(message_batch)
        message_batch = []
        last_flush_time = time.time()

consumer.close()