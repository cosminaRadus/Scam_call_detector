import random
import time
from kafka import KafkaProducer
from faker import Faker
import json

faker = Faker()

def generate_repeated_calls():
    return random.random() < 0.15  

def is_odd_hour():
    hour = random.randint(1, 24)
    if hour >= 1 and hour <= 4:  # 1 AM to 4 AM
        return True
    return False

def generate_call_data():

    call_data = {
        "call_id": faker.uuid4(),
        "source_no": faker.phone_number(),
        "destination_number": faker.phone_number(),  
        "call_duration": random.randint(1, 1200),  
        "start_time": time.time() - random.randint(1, 1000000), 
        "source_location": faker.city(),  
        "scam_flag": None
    }

    return call_data

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def send_to_kafka():
    last_call = None

    while True:
        repeat_call = generate_repeated_calls()

        if repeat_call and last_call is not None:

            call_data = last_call.copy()
            call_data["call_id"] = faker.uuid4()
            call_data["start_time"] += random.randint(30, 300)
            call_data["call_duration"] = max(1, call_data["call_duration"] + random.randint(-10, 10))
        else:
            call_data = generate_call_data()
        
        odd_hour = is_odd_hour()
        if repeat_call or odd_hour:
            call_data["scam_flag"] = 1 if random.random() < 0.7 else 0
        else:
            call_data["scam_flag"] = 0 if random.random() < 0.8 else 1
    
        last_call = call_data

        producer.send("calls_topic", call_data)
        print("Sent")
        print(json.dumps(call_data, indent=4))
        time.sleep(1)

send_to_kafka()