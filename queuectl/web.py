"""
web.py — Lightweight, Zero-Dependency Cloud Dashboard & HTTP Server for QueueCTL.
Runs a background worker thread alongside a modern web interface on Railway/Render.
"""
import os
import sys
import json
import time
import uuid
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse

from queuectl.db import get_connection, init_db
from queuectl.models import get_job_counts, get_all_workers, insert_job
from queuectl.worker import run_worker

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>QueueCTL — Distributed Background Job Engine</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    body { font-family: 'Inter', sans-serif; }
    code, pre { font-family: 'JetBrains Mono', monospace; }
  </style>
</head>
<body class="bg-slate-950 text-slate-100 min-h-screen">
  <div class="max-w-6xl mx-auto px-4 py-8">
    
    <!-- Top Header -->
    <header class="flex flex-col md:flex-row md:items-center justify-between gap-4 pb-6 border-b border-slate-800">
      <div>
        <div class="flex items-center gap-3">
          <div class="w-3 h-3 rounded-full bg-emerald-500 animate-ping"></div>
          <h1 class="text-2xl font-bold tracking-tight text-white flex items-center gap-2">
            ⚡ QueueCTL Cloud Dashboard
          </h1>
          <span class="bg-emerald-500/10 text-emerald-400 border border-emerald-500/20 text-xs px-2.5 py-0.5 rounded-full font-medium">
            Active on Railway
          </span>
        </div>
        <p class="text-slate-400 text-sm mt-1">High-concurrency background job engine with SQLite WAL mode & lease-based crash recovery.</p>
      </div>

      <div class="flex items-center gap-3">
        <button onclick="enqueueJob('sample')" class="bg-indigo-600 hover:bg-indigo-500 text-white font-medium px-4 py-2 rounded-lg text-sm transition shadow-lg shadow-indigo-600/20 active:scale-95">
          + Enqueue Task
        </button>
        <button onclick="enqueueJob('failing')" class="bg-rose-600/20 hover:bg-rose-600/30 text-rose-300 border border-rose-500/30 font-medium px-4 py-2 rounded-lg text-sm transition active:scale-95">
          + Simulate Failure (DLQ)
        </button>
      </div>
    </header>

    <!-- Metrics Cards -->
    <div class="grid grid-cols-2 md:grid-cols-5 gap-4 my-8">
      <div class="bg-slate-900/60 border border-slate-800 p-4 rounded-xl">
        <span class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Pending</span>
        <div id="cnt-pending" class="text-2xl font-bold text-amber-400 mt-1">0</div>
      </div>
      <div class="bg-slate-900/60 border border-slate-800 p-4 rounded-xl">
        <span class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Processing</span>
        <div id="cnt-processing" class="text-2xl font-bold text-sky-400 mt-1">0</div>
      </div>
      <div class="bg-slate-900/60 border border-slate-800 p-4 rounded-xl">
        <span class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Completed</span>
        <div id="cnt-completed" class="text-2xl font-bold text-emerald-400 mt-1">0</div>
      </div>
      <div class="bg-slate-900/60 border border-slate-800 p-4 rounded-xl">
        <span class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Dead (DLQ)</span>
        <div id="cnt-dead" class="text-2xl font-bold text-rose-400 mt-1">0</div>
      </div>
      <div class="bg-slate-900/60 border border-slate-800 p-4 rounded-xl col-span-2 md:col-span-1">
        <span class="text-xs font-semibold text-slate-400 uppercase tracking-wider">Total Tasks</span>
        <div id="cnt-total" class="text-2xl font-bold text-slate-100 mt-1">0</div>
      </div>
    </div>

    <!-- Active Workers Panel -->
    <div class="bg-slate-900/40 border border-slate-800 rounded-xl p-5 mb-8">
      <div class="flex items-center justify-between mb-3">
        <h2 class="text-sm font-semibold text-slate-300 uppercase tracking-wider flex items-center gap-2">
          <span>Worker Processes</span>
          <span id="worker-count-badge" class="bg-slate-800 text-slate-300 text-xs px-2 py-0.5 rounded">0 Active</span>
        </h2>
        <span class="text-xs text-slate-500 font-mono">SQLite WAL • Atomic Claims</span>
      </div>
      <div id="workers-list" class="grid grid-cols-1 md:grid-cols-3 gap-3 text-xs font-mono text-slate-400">
        <div class="p-3 bg-slate-950/60 border border-slate-800/80 rounded-lg">Connecting to workers...</div>
      </div>
    </div>

    <!-- Recent Jobs Table -->
    <div class="bg-slate-900/40 border border-slate-800 rounded-xl overflow-hidden mb-8">
      <div class="px-5 py-4 border-b border-slate-800 flex items-center justify-between">
        <h2 class="text-sm font-semibold text-slate-300 uppercase tracking-wider">Real-Time Job Queue</h2>
        <span class="text-xs text-emerald-400 flex items-center gap-1.5">
          <span class="w-2 h-2 rounded-full bg-emerald-400 animate-pulse"></span>
          Live polling every 2s
        </span>
      </div>
      <div class="overflow-x-auto">
        <table class="w-full text-left text-xs">
          <thead class="bg-slate-950/60 text-slate-400 uppercase font-mono border-b border-slate-800">
            <tr>
              <th class="px-5 py-3">Job ID</th>
              <th class="px-5 py-3">Command</th>
              <th class="px-5 py-3">State</th>
              <th class="px-5 py-3">Attempts</th>
              <th class="px-5 py-3">Timestamp</th>
            </tr>
          </thead>
          <tbody id="jobs-tbody" class="divide-y divide-slate-800/50 font-mono">
            <tr>
              <td colspan="5" class="px-5 py-8 text-center text-slate-500">Loading queue status...</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Footer Specs -->
    <footer class="text-center text-xs text-slate-600 pt-6 border-t border-slate-900">
      QueueCTL by <a href="https://github.com/jashwanth15/QueueCTL" target="_blank" class="text-indigo-400 hover:underline">jashwanth15</a> • Benchmarked at 4,800+ tasks/sec • Standard Library Only
    </footer>

  </div>

  <script>
    async function fetchStatus() {
      try {
        const res = await fetch('/api/status');
        const data = await res.json();

        // Update metric counts
        document.getElementById('cnt-pending').innerText = data.counts.pending || 0;
        document.getElementById('cnt-processing').innerText = data.counts.processing || 0;
        document.getElementById('cnt-completed').innerText = data.counts.completed || 0;
        document.getElementById('cnt-dead').innerText = data.counts.dead || 0;
        document.getElementById('cnt-total').innerText = data.total || 0;

        // Workers
        const wBadge = document.getElementById('worker-count-badge');
        wBadge.innerText = `${data.workers.length} Active`;
        const wContainer = document.getElementById('workers-list');
        if (data.workers.length === 0) {
          wContainer.innerHTML = '<div class="p-3 bg-slate-950/60 border border-slate-800/80 rounded-lg text-slate-500">No active workers</div>';
        } else {
          wContainer.innerHTML = data.workers.map(w => `
            <div class="p-3 bg-slate-950/80 border border-emerald-500/20 rounded-lg flex items-center justify-between">
              <span class="text-emerald-400">PID: ${w.pid}</span>
              <span class="text-slate-500 text-[10px]">Heartbeat: ${w.last_heartbeat.split('T')[1] || w.last_heartbeat}</span>
            </div>
          `).join('');
        }

        // Jobs Table
        const tbody = document.getElementById('jobs-tbody');
        if (data.jobs.length === 0) {
          tbody.innerHTML = '<tr><td colspan="5" class="px-5 py-8 text-center text-slate-500">No jobs enqueued yet. Click "Enqueue Task" to run one!</td></tr>';
        } else {
          tbody.innerHTML = data.jobs.map(j => {
            let badgeColor = "bg-slate-800 text-slate-300";
            if (j.state === 'completed') badgeColor = "bg-emerald-500/20 text-emerald-300 border border-emerald-500/30";
            if (j.state === 'pending') badgeColor = "bg-amber-500/20 text-amber-300 border border-amber-500/30";
            if (j.state === 'processing') badgeColor = "bg-sky-500/20 text-sky-300 border border-sky-500/30";
            if (j.state === 'dead') badgeColor = "bg-rose-500/20 text-rose-300 border border-rose-500/30";

            return `
              <tr class="hover:bg-slate-900/40 transition">
                <td class="px-5 py-3 text-indigo-300 font-semibold">${j.id}</td>
                <td class="px-5 py-3 text-slate-300">${j.command}</td>
                <td class="px-5 py-3">
                  <span class="px-2 py-0.5 rounded text-[11px] font-medium ${badgeColor}">
                    ${j.state}
                  </span>
                </td>
                <td class="px-5 py-3 text-slate-400">${j.attempts} / ${j.max_retries}</td>
                <td class="px-5 py-3 text-slate-500">${j.created_at.replace('T', ' ').slice(0, 19)}</td>
              </tr>
            `;
          }).join('');
        }

      } catch (err) {
        console.error("Error polling QueueCTL status:", err);
      }
    }

    async function enqueueJob(type) {
      try {
        await fetch('/api/enqueue?type=' + type, { method: 'POST' });
        fetchStatus();
      } catch (e) {
        alert("Failed to enqueue job: " + e);
      }
    }

    fetchStatus();
    setInterval(fetchStatus, 2000);
  </script>
</body>
</html>
"""

class QueueDashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/" or parsed.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_TEMPLATE.encode("utf-8"))

        elif parsed.path == "/api/status":
            conn = get_connection()
            counts = get_job_counts(conn)
            workers = get_all_workers(conn)
            
            # Fetch recent 15 jobs across all states
            cursor = conn.execute("""
                SELECT id, command, state, attempts, max_retries, created_at
                FROM jobs
                ORDER BY created_at DESC
                LIMIT 15
            """)
            jobs = [dict(r) for r in cursor.fetchall()]
            conn.close()

            payload = {
                "counts": counts,
                "total": sum(counts.values()),
                "workers": workers,
                "jobs": jobs
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(payload).encode("utf-8"))

        elif parsed.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status":"healthy"}')

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/enqueue":
            query_type = "sample"
            if "type=failing" in parsed.query:
                query_type = "failing"

            conn = get_connection()
            job_id = f"job-{uuid.uuid4().hex[:6]}"
            if query_type == "failing":
                cmd = "python -c 'import sys; sys.exit(1)'"
                max_retries = 2
            else:
                cmd = f"python -c 'import time; time.sleep(0.5); print(\"Task {job_id} done\")'"
                max_retries = 3

            insert_job(conn, job_id, cmd, max_retries)
            conn.close()

            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "enqueued", "id": job_id}).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

def run_server(port=8080):
    init_db()
    
    # 1. Start background worker thread
    print(f"[*] Starting QueueCTL Background Worker...")
    worker_thread = threading.Thread(target=run_worker, args=(0,), daemon=True)
    worker_thread.start()

    # 2. Start HTTP server
    server = ThreadingHTTPServer(("0.0.0.0", port), QueueDashboardHandler)
    print(f"[*] QueueCTL Web Dashboard running at http://0.0.0.0:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[*] Shutting down dashboard...")
        server.server_close()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    run_server(port)
