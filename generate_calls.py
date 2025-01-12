import random
import time
from kafka import KafkaProducer
from faker import Faker
import json

faker = Faker()

def generate_repeated_calls():
    if random.random() < 0.2:
        return True
    return False

def is_odd_hour():
    hour = random.randint(1, 24)
    if hour >= 1 and hour <= 4:  # 1 AM to 4 AM
        return True
    return False

def generate_call_data():

    reteated_call = generate_repeated_calls()
    odd_hour = is_odd_hour()

    call_data = {
        "call_id": faker.uuid4(),
        "source_no": faker.phone_number(),
        "destination_number": faker.phone_number(),  
        "call_duration": random.randint(1, 1200),  
        "start_time": time.time(), 
        "source_location": faker.city(),  
    }

    if reteated_call or odd_hour:
        call_data["scam_flag"] = 1 if random.random() < 0.7 else 0
    else:
        call_data["scam_flag"] = 0 if random.random() < 0.8 else 1

    return call_data

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# calls = [generate_call_data() for _ in range(20)]
# for call in calls:
#     print(json.dumps(call, indent=4))


def send_to_kafka():
    while True:
        call_data = generate_call_data()
        producer.send("calls_topic", call_data)
        print("Sent")
        print(json.dumps(call_data, indent=4))
        time.sleep(1)

send_to_kafka()