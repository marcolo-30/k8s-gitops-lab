# client.py
# A resilient client that ensures each job (token) is processed before starting a new one.

import os
import requests
import time
import json

# --- Configuration ---
SERVICE_ENDPOINT = os.getenv("SERVICE_ENDPOINT", "http://localhost:8080")
RETRY_DELAY_SECONDS = 2 # How long to wait between retries

def process_one_job():
    """
    This function represents a single, complete job.
    It will not exit until it receives a valid confirmation from the server
    that the specific job token has been processed.
    """
    url = f'{SERVICE_ENDPOINT}/process'
    
    # 1. Generar un Trabajo: Se crea un token único para este trabajo.
    request_token = time.time()
    payload = {"token": request_token}
    
    first_attempt_time = time.time()
    attempts = 0

    print(f"--- Starting New Job [Token: {request_token}] ---", flush=True)

    # 2. Bucle de Reintentos: Intentar procesar el trabajo hasta tener éxito.
    while True:
        attempts += 1
        print(f"--> [Attempt #{attempts}] Sending job with token {request_token}...", flush=True)
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            
            # 3. Esperar Confirmación:
            if response.status_code == 200:
                data = response.json()
                response_token = data.get("token")

                # 4. Validación del Token:
                if response_token == request_token:
                    # ¡ÉXITO! El servidor procesó y devolvió el mismo token.
                    total_duration = time.time() - first_attempt_time
                    print(f"<-- SUCCESS: Job [Token: {request_token}] confirmed.", flush=True)
                    print(f"    Total Time (including retries): {total_duration:.2f}s in {attempts} attempts.", flush=True)
                    # El trabajo está completo, salimos del bucle de reintentos.
                    return 
                else:
                    # El servidor respondió OK, pero con un token incorrecto. Es un error.
                    print(f"<-- FAILED (Token Mismatch): Expected {request_token}, Got {response_token}. Retrying...", flush=True)
            else:
                # El servidor respondió con un error HTTP (ej. 500, 503).
                print(f"<-- FAILED (HTTP {response.status_code}): Retrying...", flush=True)

        except requests.exceptions.Timeout:
            # La petición tardó demasiado.
            print(f"<-- FAILED (Timeout): Retrying...", flush=True)
        except requests.exceptions.RequestException:
            # No se pudo conectar con el servidor (puede que esté migrando).
            print(f"<-- FAILED (Connection Error): Retrying...", flush=True)
        
        # Si llegamos aquí, es porque la petición falló. Esperamos antes de reintentar.
        time.sleep(RETRY_DELAY_SECONDS)


if __name__ == "__main__":
    print(f'[CLIENT] Resilient client started. Targeting: {SERVICE_ENDPOINT}.', flush=True)
    
    # Bucle de Trabajos: Este es el bucle principal.
    # Una vez que `process_one_job()` termina (garantizando que un trabajo se completó),
    # este bucle simplemente vuelve a empezar, creando un trabajo completamente nuevo.
    while True:
        process_one_job()
        print("----------------------------------------------------")
        time.sleep(1) # Pequeña pausa antes de iniciar el siguiente trabajo.
