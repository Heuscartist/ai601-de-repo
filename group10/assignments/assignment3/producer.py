import time
import random
import json
from kafka import KafkaProducer
from prometheus_client import Counter, start_http_server, Gauge


start_http_server(8000)

traffic_volume = Counter(
    "traffic_volume_total", "Total vehicle count per sensor", ["sensor_id"]
)
average_speed_gauge = Gauge(
    "average_speed", "Average speed of vehicles per sensor", ["sensor_id"]
)

TOPIC = "traffic_data"
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sensor_ids = [101, 202, 303, 404, 505]
congestion_levels = ["LOW", "MEDIUM", "HIGH"]
max_vehicle_count = 50
max_speed = 120  

while True:
    sensor_id = random.choice(sensor_ids)
    vehicle_count = random.randint(0, max_vehicle_count)
    average_speed = round(random.uniform(5, max_speed), 2)
    event = {
        "sensor_id": sensor_id,  
        "timestamp": time.time(),
        "vehicle_count": vehicle_count,  
        "average_speed": average_speed,  
        "congestion_level": random.choice(congestion_levels) 
    }
    
    producer.send(TOPIC, event)
    
    traffic_volume.labels(sensor_id=sensor_id).inc(vehicle_count)
    average_speed_gauge.labels(sensor_id=sensor_id).set(average_speed)
    print(f"Sent event: {event}")

    
    time.sleep(random.uniform(0.5, 2.0))

