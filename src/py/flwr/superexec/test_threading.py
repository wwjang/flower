import threading
import time
import logging

# Configure the logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(threadName)s - %(message)s')

# Event for signaling threads to stop
stop_event = threading.Event()

# Lock for synchronizing access to logging
log_lock = threading.Lock()

def worker():
    while not stop_event.is_set():
        with log_lock:
            logging.info('Thread is running')
        time.sleep(1)
    with log_lock:
        logging.info('Thread is exiting gracefully')

# Create and start threads
threads = []
for i in range(3):
    thread = threading.Thread(target=worker, name=f'Worker-{i}')
    threads.append(thread)
    thread.start()

# Run the threads for 5 seconds
time.sleep(5)

# Signal threads to stop
stop_event.set()

# Wait for all threads to finish
for thread in threads:
    thread.join()

logging.info('All threads have exited gracefully')
