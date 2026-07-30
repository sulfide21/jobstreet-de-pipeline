import json
import time

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

st.set_page_config(page_title="JobStreet live stream", layout="wide")


@st.cache_resource
def get_consumer():
    """One Kafka connection, reused across Streamlit reruns."""
    return KafkaConsumer(
        "jobs",
        bootstrap_servers="localhost:9092",
        group_id="dashboard",
        auto_offset_reset="latest",  # only jobs arriving from now on
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,  # short: give control back to Streamlit
    )


st.title("JobStreet live stream")
st.caption("Reading the `jobs` Kafka topic as messages arrive")

# session_state survives reruns; a plain variable would reset every second.
if "jobs" not in st.session_state:
    st.session_state.jobs = []

consumer = get_consumer()

# Drain whatever arrived in the last second, then fall out on the timeout.
for msg in consumer:
    st.session_state.jobs.append(msg.value)

jobs = st.session_state.jobs

col1, col2, col3 = st.columns(3)
col1.metric("messages received", len(jobs))
col2.metric("distinct job ids", len({j["id"] for j in jobs}))
col3.metric("companies", len({j["company"] for j in jobs}))

if jobs:
    df = pd.DataFrame(jobs)[["id", "title", "company", "location", "salary"]]
    st.dataframe(df.iloc[::-1], use_container_width=True, height=500)
else:
    st.info("Waiting for messages. Run producer.py in another terminal.")

time.sleep(1)
st.rerun()
