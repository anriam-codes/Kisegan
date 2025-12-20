import json
from kafka import KafkaProducer
from api_fetcher import fetch_all_locations

KAFKA_BROKER = "localhost:9092"
TOPIC = "weather_raw"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def publish():
    events = fetch_all_locations()
    for event in events:
        producer.send(TOPIC, event)
    producer.flush()

if __name__ == "__main__":
    publish()
    