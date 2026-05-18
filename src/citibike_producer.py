import json
import time
import requests

from kafka import KafkaProducer

URL = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"
TOPIC = "station_status"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",
    retries=5,
)

print("Producer started. Fetching Citi Bike station status...")

while True:
    try:
        fetch_start = time.time()

        response = requests.get(URL, timeout=10)
        response.raise_for_status()

        data = response.json()
        stations = data["data"]["stations"]

        event_time = time.time()

        print(f"Fetched {len(stations)} stations")

        for station in stations:
            # Add metadata for analytics / latency tracking
            station["producer_event_time"] = event_time
            station["source"] = "citibike_gbfs_station_status"

            producer.send(TOPIC, station)

        producer.flush()

        elapsed = time.time() - fetch_start
        events_per_second = len(stations) / elapsed if elapsed > 0 else 0

        print(
            f"Sent {len(stations)} events to Kafka "
            f"in {elapsed:.2f}s "
            f"({events_per_second:.2f} events/sec)"
        )

        time.sleep(30)

    except Exception as e:
        print("Error:", e)
        time.sleep(10)