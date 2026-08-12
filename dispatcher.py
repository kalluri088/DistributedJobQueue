import socket
import threading
import json
import queue
import time
import ssl

# ─────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────
DISPATCHER_HOST = '0.0.0.0'   # listen on all interfaces
DISPATCHER_PORT = 5000         # clients connect here
WORKER_PORT     = 6000         # workers connect here
MAX_CLIENTS     = 10
MAX_WORKERS     = 10

# ─────────────────────────────────────────────
#  SHARED STATE  (protected by locks)
# ─────────────────────────────────────────────
job_queue    = queue.Queue()          # pending jobs (thread-safe)
job_results  = {}                     # job_id -> result dict
job_lock     = threading.Lock()       # protects job_results
worker_list  = []                     # connected worker sockets
worker_lock  = threading.Lock()       # protects worker_list
job_counter  = 0
counter_lock = threading.Lock()

# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
def send_json(sock, data):
    """Send a JSON-encoded message, prefixed with 4-byte length."""
    msg = json.dumps(data).encode()
    length = len(msg).to_bytes(4, 'big')
    sock.sendall(length + msg)

def recv_json(sock):
    """Receive a length-prefixed JSON message."""
    raw_len = _recv_exact(sock, 4)
    if not raw_len:
        return None
    length = int.from_bytes(raw_len, 'big')
    raw_msg = _recv_exact(sock, length)
    if not raw_msg:
        return None
    return json.loads(raw_msg.decode())

def _recv_exact(sock, n):
    """Read exactly n bytes from socket."""
    buf = b''
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf

def new_job_id():
    global job_counter
    with counter_lock:
        job_counter += 1
        return f"JOB-{job_counter:03d}"

# ─────────────────────────────────────────────
#  CLIENT HANDLER  (one thread per client)
# ─────────────────────────────────────────────
def handle_client(conn, addr):
    print(f"  [CLIENT] Connected from {addr}")
    try:
        while True:
            msg = recv_json(conn)
            if not msg:
                break

            action = msg.get('action')

            # ── Submit a new job ──────────────────────
            if action == 'submit':
                job_id = new_job_id()
                with job_lock:
                    job_results[job_id] = {'status': 'QUEUED', 'result': None}

                job = {
                    'job_id':   job_id,
                    'tasks':    msg['tasks'],
                    'filedata': msg['filedata'],
                }
                job_queue.put(job)
                print(f"  [QUEUE]  {job_id} queued  | tasks: {msg['tasks']}")
                send_json(conn, {'status': 'ACCEPTED', 'job_id': job_id})

            # ── Check status of an existing job ───────
            elif action == 'status':
                job_id = msg.get('job_id')
                with job_lock:
                    info = job_results.get(job_id)
                if info:
                    send_json(conn, {'status': info['status'], 'result': info['result']})
                else:
                    send_json(conn, {'status': 'NOT_FOUND', 'result': None})

            else:
                send_json(conn, {'status': 'ERROR', 'message': 'Unknown action'})

    except Exception as e:
        print(f"  [ERROR]  Client handler: {e}")
    finally:
        conn.close()
        print(f"  [CLIENT] Disconnected: {addr}")

# ─────────────────────────────────────────────
#  WORKER HANDLER  (one thread per worker)
# ─────────────────────────────────────────────
def handle_worker(conn, addr):
    print(f"  [WORKER] Connected from {addr}")
    with worker_lock:
        worker_list.append(conn)
    try:
        while True:
            # Block until a job is available
            job = job_queue.get()
            job_id = job['job_id']

            # Mark as in-progress
            with job_lock:
                job_results[job_id]['status'] = 'IN_PROGRESS'
            print(f"  [ASSIGN] {job_id} → worker at {addr}")

            # Send job to worker
            send_json(conn, job)

            # Wait for result
            result_msg = recv_json(conn)
            if result_msg is None:
                # Worker disconnected — requeue the job
                print(f"  [WARN]   Worker lost, re-queuing {job_id}")
                with job_lock:
                    job_results[job_id]['status'] = 'QUEUED'
                job_queue.put(job)
                break

            # Store result
            with job_lock:
                job_results[job_id]['status'] = 'DONE'
                job_results[job_id]['result'] = result_msg.get('result')
            print(f"  [DONE]   {job_id} completed")

    except Exception as e:
        print(f"  [ERROR]  Worker handler: {e}")
    finally:
        with worker_lock:
            if conn in worker_list:
                worker_list.remove(conn)
        conn.close()
        print(f"  [WORKER] Disconnected: {addr}")

# ─────────────────────────────────────────────
#  LISTENER THREADS
# ─────────────────────────────────────────────
def listen_for_clients():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((DISPATCHER_HOST, DISPATCHER_PORT))
    server.listen(MAX_CLIENTS)
    print(f"  Listening for CLIENTS  on port {DISPATCHER_PORT} ...")
    while True:
        conn, addr = server.accept()
        t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        t.start()

def listen_for_workers():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((DISPATCHER_HOST, WORKER_PORT))
    server.listen(MAX_WORKERS)
    print(f"  Listening for WORKERS  on port {WORKER_PORT} ...")
    while True:
        conn, addr = server.accept()
        t = threading.Thread(target=handle_worker, args=(conn, addr), daemon=True)
        t.start()

# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
if __name__ == '__main__':
    print("=" * 52)
    print("   DISTRIBUTED JOB QUEUE — DISPATCHER")
    print("=" * 52)

    t1 = threading.Thread(target=listen_for_clients, daemon=True)
    t2 = threading.Thread(target=listen_for_workers, daemon=True)
    t1.start()
    t2.start()

    print("  Dispatcher is running. Press Ctrl+C to stop.\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n  Dispatcher shut down.")