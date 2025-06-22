from producer import message_queue
from collections import defaultdict
import time, json
import os
import csv
from pymongo import MongoClient

#Initialize counters for tracking user and item interactions
user_interactions = defaultdict(int)    #Maps user_id → number of interactions   
item_interactions = defaultdict(int)    # Maps item_id → number of interactions
records_processed = 0                   # Counter for total records processed

# Function to store aggregated results into MongoDB Atlas database
def save_to_mongo(results):
    try:
        uri = os.getenv("MONGO_URI")        # Get MongoDB URI from environment variable
        if not uri:
            print("⚠️  MONGO_URI not found in environment variables")
            return
            
        client = MongoClient(uri)           # Connect to MongoDB
        db = client["streaming_db"]         # Select database
        stats = db["aggregates"]            # Select collection for storing aggregates

        stats.insert_one(results)           # Insert the results into the collection
        print("✅ Aggregated results stored in MongoDB Atlas")
    except Exception as e:
        print(f"⚠️  Failed to save to MongoDB: {e}")
        print("📄 Results will still be saved to CSV file")

def save_to_csv(results, filename="aggregates_output.csv"):
    fieldnames = results.keys()
    file_exists = os.path.isfile(filename)

    with open(filename, mode="a", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()  # write headers if file doesn't exist yet
        writer.writerow(results)

    print("📄 Aggregated results stored in CSV file")
    
# Function to consume messages from the queue and perform aggregations
def consume(message_queue):
    global records_processed                    # Access global record counter

    print("\n--- Starting Consumer ---")        
    while not message_queue.empty():            # Loop until the queue is empty
        record = message_queue.get()            # Get the next message from the queue
        records_processed += 1                  # Increment the record counter

        uid = record["user_id"]                 # Extract user_id from the message
        iid = record["item_id"]                 # Extract item_id from the message

        user_interactions[uid] += 1             # Increment user interaction count
        item_interactions[iid] += 1             # Increment item interaction count  

        time.sleep(0.01)                        # Simulate small processing delay (10ms)


# Calculating final aggregation metrics after all records are consumed
    if user_interactions:
        avg = round(
            sum(user_interactions.values()) / len(user_interactions), 2)    # Avg interactions per user
        max_i = max(item_interactions.values())                             # Max interactions per item
        min_i = min(item_interactions.values())                             # Min interactions per item
    else:
        avg = max_i = min_i = 0

    print("\n Aggregation Complete")
    print(f"Total Records: {records_processed}")
    print(f"Avg Interactions per User: {avg}")
    print(f"Max Interactions per Item: {max_i}")
    print(f"Min Interactions per Item: {min_i}")

# Returning results to save it to a CSV file
    result_doc = {
        "average_interactions_per_user": avg,
        "max_per_item": max_i,
        "min_per_item": min_i,
        "total_records": records_processed,
        "timestamp": time.time()
    }

    save_to_mongo(result_doc)
    save_to_csv(result_doc)       # Logs to aggregates_output.csv

    return result_doc