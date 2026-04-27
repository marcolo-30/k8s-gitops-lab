# client.py
# A resilient client that retries until it gets a 200 OK from the server.

import os
import requests
import time
import json

# --- Configuration ---
SERVICE_ENDPOINT = os.getenv("SERVICE_ENDPOINT", "http://localhost:8080")
RETRY_DELAY_SECONDS = 2

def process_one_job():
    url = f'{SERVICE_ENDPOINT}/process'
    
    first_attempt_time = time.time()
    attempts = 0

    print(f"--- Starting New Job ---", flush=True)

    while True:
        attempts += 1
        print(f"--> [Attempt #{attempts}] Sending processing request...", flush=True)
        
        try:
            response = requests.post(url, timeout=10)
            
            if response.status_code == 200:
                # SUCCESS! The server guarantees the work is done.
                total_duration = time.time() - first_attempt_time
                data = response.json()
                print(f"<-- SUCCESS: Job confirmed by server.", flush=True)
                print(f"    Total Time: {total_duration:.2f}s in {attempts} attempts.", flush=True)
                print(f"    Server-side duration: {data.get('processing_time_seconds', 0):.2f}s", flush=True)
                return
            else:
                # The server responded with an error (e.g., 500 for incomplete work).
                print(f"<-- FAILED (Server Error {response.status_code}): Retrying...", flush=True)

        except requests.exceptions.Timeout:
            print(f"<-- FAILED (Timeout): Retrying...", flush=True)
        except requests.exceptions.RequestException:
            print(f"<-- FAILED (Connection Error): Retrying...", flush=True)
        
        time.sleep(RETRY_DELAY_SECONDS)


if __name__ == "__main__":
    print(f'[CLIENT] Resilient client started. Targeting: {SERVICE_ENDPOINT}.', flush=True)
    while True:
        process_one_job()
        print("----------------------------------------------------")
        time.sleep(1)
