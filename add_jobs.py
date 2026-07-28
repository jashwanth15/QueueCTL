import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from queuectl.db import init_db, get_connection
from queuectl.models import insert_job

def main():
    init_db()
    conn = get_connection()
    
    # 1. The Fast Job
    try:
        insert_job(conn, 'job-fast', 'echo Hello from the fast job!', 3)
        print("Added: job-fast")
    except Exception:
        pass # Already exists
    
    # 2. The Slow Job
    try:
        insert_job(conn, 'job-slow', 'python -c "import time; time.sleep(3); print(\'Slow job done\')"', 3)
        print("Added: job-slow")
    except Exception:
        pass # Already exists
        
    # 3. The Broken Job
    try:
        insert_job(conn, 'job-broken', 'exit 1', 2)
        print("Added: job-broken")
    except Exception:
        pass # Already exists
    
    conn.close()
    print("All jobs queued!")

if __name__ == "__main__":
    main()
