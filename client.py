import socket
import json
import time
import os
import ssl

# ─────────────────────────────────────────────
#  CONFIGURATION  — change IP if dispatcher is
#  on a different machine (e.g. your Linux VM)
# ─────────────────────────────────────────────
DISPATCHER_HOST = '10.134.80.123'   # replace with VM IP if needed
DISPATCHER_PORT = 5000
POLL_INTERVAL   = 1.5            # seconds between status checks

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
#  UI HELPERS
# ─────────────────────────────────────────────
def divider():
    print("─" * 54)

def header(title):
    print("=" * 54)
    print(f"   {title}")
    print("=" * 54)

def print_results(job_id, result):
    print()
    header(f"RESULTS FOR {job_id}")

    if 'wordcount' in result:
        print(f"  Word count     : {result['wordcount']}")

    if 'lowercase' in result:
        text = result['lowercase']
        preview = text[:80] + ('...' if len(text) > 80 else '')
        print(f"  Lowercase text : {preview}")

    if 'top_words' in result:
        words = result['top_words']
        formatted = '  '.join(f"{w}({c})" for w, c in words)
        print(f"  Top words      : {formatted}")

    print("=" * 54)
    print()

# ─────────────────────────────────────────────
#  TASK SELECTION
# ─────────────────────────────────────────────
def choose_tasks():
    divider()
    print("  Select tasks to run:")
    print("    [1] Word count")
    print("    [2] Convert to lowercase")
    print("    [3] Word frequency (top 5)")
    print("    [4] All of the above")
    divider()

    task_map = {
        '1': ['wordcount'],
        '2': ['lowercase'],
        '3': ['frequency'],
        '4': ['wordcount', 'lowercase', 'frequency'],
    }

    while True:
        choice = input("  Your choice (1-4): ").strip()
        if choice in task_map:
            return task_map[choice]
        print("  Invalid choice. Please enter 1, 2, 3, or 4.")

# ─────────────────────────────────────────────
#  SUBMIT JOB
# ─────────────────────────────────────────────
def submit_job(sock, filedata, tasks):
    send_json(sock, {
        'action':   'submit',
        'tasks':    tasks,
        'filedata': filedata,
    })
    response = recv_json(sock)
    if response and response.get('status') == 'ACCEPTED':
        return response['job_id']
    return None

# ─────────────────────────────────────────────
#  POLL STATUS
# ─────────────────────────────────────────────
def poll_status(sock, job_id):
    spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
    spin_i  = 0

    while True:
        send_json(sock, {'action': 'status', 'job_id': job_id})
        response = recv_json(sock)

        if not response:
            print("\n  [ERROR] Lost connection to dispatcher.")
            return None

        status = response.get('status')

        # Overwrite the same line for a live feel
        print(f"\r  {spinner[spin_i % len(spinner)]}  {job_id}  :  {status}          ", end='', flush=True)
        spin_i += 1

        if status == 'DONE':
            print()  # newline after spinner
            return response.get('result')

        if status == 'NOT_FOUND':
            print(f"\n  [ERROR] Job {job_id} not found on dispatcher.")
            return None

        time.sleep(POLL_INTERVAL)

# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    header("DISTRIBUTED JOB QUEUE — CLIENT")

    # ── Step 1: Load file ─────────────────────
    while True:
        filepath = input("\n  Enter path to your .txt file: ").strip()
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                filedata = f.read()
            size = os.path.getsize(filepath)
            print(f"  File loaded: {filepath}  ({size} bytes, {len(filedata.split())} words)")
            break
        else:
            print(f"  File not found: '{filepath}'. Try again.")

    # ── Step 2: Choose tasks ──────────────────
    print()
    tasks = choose_tasks()
    print(f"  Tasks selected: {tasks}")

    # ── Step 3: Connect to dispatcher ─────────
    print(f"\n  Connecting to dispatcher at {DISPATCHER_HOST}:{DISPATCHER_PORT} ...")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((DISPATCHER_HOST, DISPATCHER_PORT))
        print("  Connected!")
    except ConnectionRefusedError:
        print("  [ERROR] Could not connect to dispatcher. Is it running?")
        return

    # ── Step 4: Submit job ────────────────────
    print("\n  Submitting job ...")
    job_id = submit_job(sock, filedata, tasks)
    if not job_id:
        print("  [ERROR] Dispatcher rejected the job.")
        sock.close()
        return
    print(f"  Job accepted!  ID: {job_id}")

    # ── Step 5: Poll for result ───────────────
    print(f"\n  Waiting for result ...\n")
    result = poll_status(sock, job_id)

    # ── Step 6: Display result ────────────────
    if result:
        print_results(job_id, result)
    else:
        print("  No result received.")

    sock.close()

    # ── Step 7: Run another job? ──────────────
    again = input("  Submit another job? (y/n): ").strip().lower()
    if again == 'y':
        main()
    else:
        print("\n  Goodbye!\n")

if __name__ == '__main__':
    main()