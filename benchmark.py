"""
benchmark.py — High-Throughput Performance & Concurrency Benchmark for QueueCTL
Tests SQLite WAL mode batch ingestion and concurrent worker job-claiming throughput.
"""
import time
import os
import sqlite3
import tempfile
from concurrent.futures import ThreadPoolExecutor

def run_benchmark(total_jobs=10000, num_workers=4):
    print("================================================================")
    print("            QueueCTL Performance & Concurrency Benchmark         ")
    print("================================================================")
    print(f"[*] Target Tasks: {total_jobs:,}")
    print(f"[*] Worker Threads: {num_workers}")
    print(f"[*] Journal Mode: Write-Ahead Logging (WAL)")

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        db_path = os.path.join(tmpdir, "bench_queue.db")
        conn = sqlite3.connect(db_path, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                command TEXT NOT NULL,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 3,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                lease_expires_at TEXT,
                worker_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_jobs_state_lease ON jobs (state, lease_expires_at);
        """)
        conn.commit()

        # Phase 1: Ingestion / Enqueue Throughput
        print("\n--- Phase 1: High-Speed Batch Enqueue ---")
        jobs_data = [
            (f"bench_job_{i}", f"echo 'payload_{i}'", "pending", 0, 3, "2026-09-09T00:00:00Z", "2026-09-09T00:00:00Z")
            for i in range(total_jobs)
        ]

        t0 = time.perf_counter()
        with conn:
            conn.executemany(
                "INSERT INTO jobs (id, command, state, attempts, max_retries, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                jobs_data
            )
        t_enqueue = time.perf_counter() - t0
        enqueue_rate = total_jobs / t_enqueue
        print(f"[+] Enqueued {total_jobs:,} jobs in {t_enqueue:.3f}s -> {enqueue_rate:,.1f} tasks/sec")
        conn.close()

        # Phase 2: Concurrent Atomic Worker Claiming
        print("\n--- Phase 2: Concurrent Worker Claiming & Execution ---")

        def worker_task(worker_id):
            w_conn = sqlite3.connect(db_path, timeout=30)
            w_conn.execute("PRAGMA journal_mode=WAL;")
            w_conn.execute("PRAGMA synchronous=NORMAL;")
            claimed_by_worker = 0
            
            while True:
                try:
                    w_conn.execute("BEGIN IMMEDIATE")
                    cursor = w_conn.execute(
                        "SELECT id FROM jobs WHERE state = 'pending' LIMIT 50"
                    )
                    rows = cursor.fetchall()
                    if not rows:
                        w_conn.execute("COMMIT")
                        break
                    
                    ids = [r[0] for r in rows]
                    placeholders = ",".join("?" for _ in ids)
                    w_conn.execute(
                        f"UPDATE jobs SET state = 'completed', worker_id = ? WHERE id IN ({placeholders})",
                        [f"worker-{worker_id}"] + ids
                    )
                    w_conn.execute("COMMIT")
                    claimed_by_worker += len(ids)
                except sqlite3.OperationalError:
                    w_conn.rollback()
                    time.sleep(0.001)
            
            w_conn.close()
            return claimed_by_worker

        t1 = time.perf_counter()
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            results = list(executor.map(worker_task, range(num_workers)))
        t_claim = time.perf_counter() - t1

        total_processed = sum(results)
        claim_rate = total_processed / t_claim
        avg_latency_ms = (t_claim / total_processed) * 1000

        print(f"[+] Processed {total_processed:,} jobs across {num_workers} workers in {t_claim:.3f}s")
        print(f"[+] Effective Claim Throughput: {claim_rate:,.1f} tasks/sec")
        print(f"[+] Average Latency: {avg_latency_ms:.3f} ms / task")
        print("\n================================================================")
        print("               QueueCTL Benchmark Completed Successfully        ")
        print("================================================================")

if __name__ == "__main__":
    run_benchmark(total_jobs=10000, num_workers=4)
