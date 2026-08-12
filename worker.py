import socket
import json
import sys
import time
from collections import Counter
import ssl

# ─────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────
DISPATCHER_HOST = '127.0.0.1'  # change to dispatcher's IP if on different machine
WORKER_PORT     = 6000

# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
def send_json(sock, data):
    msg = json.dumps(data).encode()
    length = len(msg).to_bytes(4, 'big')
    sock.sendall(length + msg)

def recv_json(sock):
    raw_len = _recv_exact(sock, 4)
    if not raw_len:
        return None
    length = int.from_bytes(raw_len, 'big')
    raw_msg = _recv_exact(sock, length)
    if not raw_msg:
        return None
    return json.loads(raw_msg.decode())

def _recv_exact(sock, n):
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf

# ─────────────────────────────────────────────
#  TASK PROCESSORS
# ─────────────────────────────────────────────
def count_words(text):
    """Count total number of words."""
    words = text.split()
    return len(words)

def to_lowercase(text):
    """Convert all uppercase characters to lowercase."""
    return text.lower()

def word_frequency(text, top_n=5):
    """Find the top N most frequent words."""
    words = text.lower().split()
    # Remove basic punctuation from words
    cleaned = []
    for w in words:
        w = w.strip('.,!?;:\'"()[]{}')
        if w:
            cleaned.append(w)
    counter = Counter(cleaned)
    return counter.most_common(top_n)

def process_job(job):
    """Run all requested tasks and return results."""
    filedata = job['filedata']
    tasks    = job['tasks']
    result   = {}

    if 'wordcount' in tasks:
        result['wordcount'] = count_words(filedata)

    if 'lowercase' in tasks:
        result['lowercase'] = to_lowercase(filedata)

    if 'frequency' in tasks:
        top_words = word_frequency(filedata)
        # Convert to list of [word, count] for JSON serialisation
        result['top_words'] = [[w, c] for w, c in top_words]

    return result

# ─────────────────────────────────────────────
#  MAIN WORKER LOOP
# ─────────────────────────────────────────────
def run_worker(worker_name):
    print("=" * 52)
    print(f"   DISTRIBUTED JOB QUEUE — {worker_name}")
    print("=" * 52)
    print(f"  Connecting to dispatcher at {DISPATCHER_HOST}:{WORKER_PORT} ...")

    # Wrap the worker socket with SSL
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_cert_chain(certfile='worker.crt', keyfile='worker.key')  # Use worker cert/key
    context.load_verify_locations('ca.crt')  # Load CA certificate
    context.check_hostname = False  # Disable hostname verification (optional)

    while True:   # auto-reconnect loop
        try:
            worker_socket = socket.create_connection((DISPATCHER_HOST, WORKER_PORT))
            with context.wrap_socket(worker_socket, server_hostname=DISPATCHER_HOST) as secure_socket:
                print("Connected to dispatcher with SSL/TLS...")
                # Send and receive data
                while True:
                    job = recv_json(secure_socket)
                    if job is None:
                        print("  Connection to dispatcher lost.")
                        break

                    job_id = job['job_id']
                    tasks  = job['tasks']
                    print(f"  [RECV]   {job_id} | tasks: {tasks}")

                    # Process
                    result = process_job(job)
                    print(f"  [DONE]   {job_id} processed successfully")

                    # Send result back
                    send_json(secure_socket, {'job_id': job_id, 'result': result})

        except ConnectionRefusedError:
            print("  Dispatcher not reachable. Retrying in 3 seconds...")
            time.sleep(3)
        except Exception as e:
            print(f"  [ERROR] {e}. Retrying in 3 seconds...")
            time.sleep(3)

# ─────────────────────────────────────────────
#  ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == '__main__':
    # Optional: pass a worker name as argument e.g. python worker.py Worker-1
    name = sys.argv[1] if len(sys.argv) > 1 else 'WORKER'
    run_worker(name)