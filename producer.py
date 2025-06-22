from data_generator import generate_log
import threading, queue, time, json

# Simulated Kafka topic
message_queue = queue.Queue()

# Parameters
RATE_PER_SEC = 20       # Messages per second
TOTAL_MESSAGES = 1000   # How many to send

def produce(rate=RATE_PER_SEC, total=TOTAL_MESSAGES):
    for _ in range(total):
        record = generate_log()
        message_queue.put(record)
        print("Produced:", json.dumps(record))
        time.sleep(1 / rate)

# Kick off producer thread
if __name__ == "__main__":
    producer_thread = threading.Thread(target=produce)
    producer_thread.start()