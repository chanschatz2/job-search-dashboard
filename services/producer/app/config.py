# KAFKA PRODUCER CONFIG
import os

# source from env or default
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092") 
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_RAW", "job_postings_raw")
JSONL_PATH = os.getenv("JSONL_PATH", "/app/data/sample_jobs.jsonl") # input path

SLEEP_SECONDS = float(os.getenv("PRODUCER_SLEEP_SECONDS", "1.0"))