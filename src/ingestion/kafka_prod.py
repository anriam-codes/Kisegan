import json
from kafka import KafkaProducer
from api_fetcher import fetch_weather

KAFKA_BROKER = "localhost:9092"
TOPIC = "weather_raw"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def publish():
    raw_event = fetch_weather()
    producer.send(TOPIC, raw_event)
    producer.flush()

if __name__ == "__main__":
    publish()
