from producer import produce, message_queue
from consumer import consume
import threading
import os

def run_together():
    producer_thread = threading.Thread(target=produce, kwargs={"rate": 20, "total": 100})
    consumer_thread = threading.Thread(target=consume, args=(message_queue,))

    producer_thread.start()
    producer_thread.join()  # Wait for producer to finish
    consumer_thread.start()
    consumer_thread.join()

def run_dashboard():
    os.system("streamlit run dashboard.py")

if __name__ == "__main__":
    run_together()