from kafka import KafkaProducer
import json
from datetime import datetime, timezone


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

scrape_date = "2026-07-27"

with open(f"../data_lake/raw/jobstreet/date={scrape_date}/jobs.json", encoding="utf-8") as f:
    jobs = json.load(f)
    
for job in jobs:
    job["scrape_date"] = scrape_date
    job["ingested_at"] = datetime.now(timezone.utc).isoformat()
    producer.send("jobs", key=job["id"], value=job)
    print(job["id"],job["title"])


producer.flush()                
print(f"sent {len(jobs)} jobs")

