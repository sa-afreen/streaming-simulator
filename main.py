from producer import produce, message_queue
from consumer import consume
import threading
import os

# Function to run producer and consumer sequentially using threads
def run_together():
    # Create thread to generate mock events at rate 20/s, total 100
    producer_thread = threading.Thread(target=produce, kwargs={"rate": 20, "total": 100})
    # Create thread to consume/process messages from the queue
    consumer_thread = threading.Thread(target=consume, args=(message_queue,))

    producer_thread.start()
    producer_thread.join()  
    consumer_thread.start()
    consumer_thread.join()

# Entry point of the script; runs the data pipeline
if __name__ == "__main__":
    run_together()