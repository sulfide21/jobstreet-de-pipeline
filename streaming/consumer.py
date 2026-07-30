from kafka import KafkaConsumer
import duckdb, json

consumer = KafkaConsumer(
    "jobs",
    bootstrap_servers="localhost:9092",
    group_id="jobs-to-duckdb",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=60000,
)

con = duckdb.connect("../warehouse/jobstreet.duckdb")
con.execute("create schema if not exists stream")
con.execute("""
    create table if not exists stream.jobs_live (
        id varchar primary key, title varchar, company varchar, salary varchar,
        location varchar, work_type varchar, listing_date varchar, scrape_date date, ingested_at timestamp
    )
""")

count = 0
for msg in consumer:
    job = msg.value
    con.execute(
        """insert into stream.jobs_live values (?, ?, ?, ?, ?, ?, ?, ?, ?) 
            on conflict (id) do update set
                title = excluded.title,
                company = excluded.company,
                salary = excluded.salary,
                location = excluded.location,
                work_type = excluded.work_type,
                listing_date = excluded.listing_date,
                scrape_date = excluded.scrape_date
                """,
        [
            job["id"],
            job["title"],
            job["company"],
            job["salary"],
            job["location"],
            job["work_type"],
            job["listing_date"],
            job.get("scrape_date"),
            job.get("ingested_at"),
        ],
    )
    count += 1
    print(f"{count:4d}  {job['id']}  {job['title'][:50]}")
    
print(f"consumed {count} messages")
print(con.execute("select count(*) from stream.jobs_live").fetchone())
consumer.close()
con.close()

