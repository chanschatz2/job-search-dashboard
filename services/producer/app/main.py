import json
import time
import uuid
from datetime import datetime, timezone
import sys

from .config import KAFKA_BOOTSTRAP, KAFKA_TOPIC, JSONL_PATH, SLEEP_SECONDS
from confluent_kafka import Producer

def iter_jsonl(path: str):
    # Generator func for input stream
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            print(line)
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                sys.stderr.write(f"Error decoding JSON in producer to topic: {KAFKA_TOPIC}\n")

# Create event from raw job input dict
def make_event(raw_posting: dict) -> dict:
    return {
        "event_id": str(uuid.uuid4()), # create uuid for event
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "jsonl",
        "posting": raw_posting,
    }

def build_producer():
    # Config producer
    conf = {'bootstrap.servers': KAFKA_BOOTSTRAP}
    return Producer(conf)

def delivery_callback(err, msg):
    if err is not None:
        sys.stderr.write(f"Producer delivery failed: {err} in topic: {KAFKA_TOPIC}\n")
        return

def send(producer, topic: str, event: dict):
    json_bytes = json.dumps(event).encode('utf-8')
    producer.produce(topic, value=json_bytes, key=event["event_id"], callback=delivery_callback)
        # partitioning key optional
    producer.poll(0)

def main():
    print(f"{KAFKA_BOOTSTRAP}\n{KAFKA_TOPIC}\n{JSONL_PATH}", flush=True)
    p = build_producer()

    # cycle through jsonl input
    while True: # Keep producer alive for now, TODO remove
        for posting in iter_jsonl(JSONL_PATH):
            event = make_event(posting)
            send(p, KAFKA_TOPIC, event) # send to p buff
            time.sleep(SLEEP_SECONDS)

        # Ensure producer buffer is cleared
        p.flush()

if __name__ == "__main__":
    main()
