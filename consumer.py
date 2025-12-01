from kafka import KafkaConsumer
import os
import json

consumer = KafkaConsumer(
    "weather-readings",
    bootstrap_servers = "localhost:9092", 
    value_deserializer = lambda x : json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest"
)

os.makedirs("../data", exist_ok=True)
DATA_FILE = "../data/readings.json"

buffer = []
print('Listening for updates')

for message in consumer:
    data = message.value
    buffer.append(data)

    if len(buffer) > 5 :
        buffer.pop()
    
    with open(DATA_FILE, 'w') as f :
        f.write(json.dumps(buffer))
    
    print('received', data)
