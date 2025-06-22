from data_generator import generate_log    #Import function to create synthetic interaction logs
import threading, queue, time, json        #Import modules for threading, message queuing, timing, and JSON formatting

# Simulated Kafka topic using a thread-safe queue
message_queue = queue.Queue()

# Configuration parameters for message generation
RATE_PER_SEC = 20       # Messages per second
TOTAL_MESSAGES = 1000   # How many to send

# Function to generate and queue messages at a given rate
def produce(rate=RATE_PER_SEC, total=TOTAL_MESSAGES):
    for _ in range(total):
        record = generate_log()                            # Create a synthetic interaction log
        message_queue.put(record)                          # Add it to the message queue
        print("Produced:", json.dumps(record))             # Print the log as a formatted JSON string
        time.sleep(1 / rate)                               # Wait to control message production rate    

# Run producer in a separate thread when executed as main script
if __name__ == "__main__":
    producer_thread = threading.Thread(target=produce)  # Create a thread for the producer
    producer_thread.start()                             # Start the thread