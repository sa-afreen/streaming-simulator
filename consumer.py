from producer import message_queue
from collections import defaultdict
import time, json
import os
from pymongo import MongoClient

# Aggregation structures
user_interactions = defaultdict(int)
item_interactions = defaultdict(int)
records_processed = 0


def save_to_mongo(results):
    uri = os.getenv("MONGO_URI")
    client = MongoClient(uri)
    db = client["streaming_db"]
    stats = db["aggregates"]

    stats.insert_one(results)
    print("✅ Aggregated results stored in MongoDB Atlas")


def consume(message_queue):
    global records_processed

    print("\n--- Starting Consumer ---")
    while not message_queue.empty():
        record = message_queue.get()
        records_processed += 1

        uid = record["user_id"]
        iid = record["item_id"]

        user_interactions[uid] += 1
        item_interactions[iid] += 1

        time.sleep(0.01)  # Simulate processing delay

    # --- Final Aggregation Metrics ---
    if user_interactions:
        avg = round(
            sum(user_interactions.values()) / len(user_interactions), 2)
        max_i = max(item_interactions.values())
        min_i = min(item_interactions.values())
    else:
        avg = max_i = min_i = 0

    print("\n✅ Aggregation Complete")
    print(f"Total Records: {records_processed}")
    print(f"Avg Interactions per User: {avg}")
    print(f"Max Interactions per Item: {max_i}")
    print(f"Min Interactions per Item: {min_i}")

    save_to_mongo({
        "average_interactions_per_user": avg,
        "max_per_item": max_i,
        "min_per_item": min_i,
        "total_records": records_processed,
        "timestamp": time.time()
    })
    
    return {
        "average_interactions_per_user": avg,
        "max_per_item": max_i,
        "min_per_item": min_i,
        "total_records": records_processed,
        "timestamp": time.time()
    }
