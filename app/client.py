# client.py
# A simple client to make requests to the image processing microservice.

import os
import requests
import time
import json

# --- Configuration ---
# For local testing, you can use the NodePort URL.
# For in-cluster testing, use the service DNS name: http://image-processor-svc:8080
SERVICE_ENDPOINT = os.getenv("SERVICE_ENDPOINT", "http://localhost:8080")

def make_request():
    url = f'{SERVICE_ENDPOINT}/process'
    print(f'--> [CLIENT] Sending request to {url}', flush=True)
    try:
        start_time = time.time()
        response = requests.post(url, timeout=30)
        duration = time.time() - start_time
        if response.status_code == 200:
            data = response.json()
            print(f'<-- [CLIENT] SUCCESS | Status: {response.status_code} | App Duration: {data.get("processing_time_seconds", 0):.2f}s | Total RTT: {duration:.2f}s', flush=True)
        else:
            print(f'<-- [CLIENT] FAILED  | Status: {response.status_code} | Total RTT: {duration:.2f}s', flush=True)
    except requests.exceptions.Timeout:
        print(f'<-- [CLIENT] TIMEOUT | Request took more than 30 seconds.', flush=True)
    except requests.exceptions.RequestException as e:
        print(f'<-- [CLIENT] ERROR   | Could not connect to service: {e}', flush=True)

if __name__ == "__main__":
    if "localhost" in SERVICE_ENDPOINT:
        print("\n[WARN] SERVICE_ENDPOINT is not set. Using default localhost.")
        print("       For in-cluster, set to: http://image-processor-svc:8080")
        print("       For external, set to: http://<node-ip>:<node-port>\n")

    print(f'[CLIENT] Client started. Targeting endpoint: {SERVICE_ENDPOINT}', flush=True)
    while True:
        make_request()
        time.sleep(2)
