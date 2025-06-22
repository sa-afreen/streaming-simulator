import streamlit as st
from pymongo import MongoClient
import pandas as pd
import os
from datetime import datetime

# MongoDB connection
uri = os.getenv("MONGO_URI")
client = MongoClient(uri)
db = client["streaming_db"]
collection = db["aggregates"]

# Fetch data
records = list(collection.find().sort("timestamp", -1))  # Most recent first

# Convert to DataFrame
if records:
    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s')
else:
    st.warning("No aggregation data found yet.")
    st.stop()

# Dashboard
st.title("📊 Real-Time Streaming Dashboard")

st.metric("Average Interactions per User", df.iloc[0]["average_interactions_per_user"])
st.metric("Maximum Interactions per Item", df.iloc[0]["max_per_item"])
st.metric("Minimum Interactions per Item", df.iloc[0]["min_per_item"])
st.text(f"Total Records Processed: {df.iloc[0]['total_records']}")
st.text(f"Last Update: {df.iloc[0]['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")

# Optional: Historical Trends
st.subheader("📈 Aggregation Over Time")
st.line_chart(df[["timestamp", "average_interactions_per_user"]].set_index("timestamp"))