from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    # server where kafka is running
    bootstrap_servers = "localhost:9092", 
    # kafka accepts data in byte so we are converting that here
    value_serializer = lambda x : json.dumps(x).encode("utf-8")
)

# to simulate real time sensor
while True:
    data = {
        "temperature": round(random.uniform(20, 50), 1),
        "humidity": round(random.uniform(10, 90), 1),
        "rainfall": round(random.uniform(0, 120), 1)
    }

    producer.send("weather-readings", data)
    print('sent data', data)
    time.sleep(2)
