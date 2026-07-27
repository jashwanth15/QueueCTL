"""
dashboard.py — Zero-dependency live visualizer for QueueCTL.

Run this script to start a local web server that shows a live view
of the jobs and workers in the database.

Usage:
    python dashboard.py
"""
import sys
import os
import json
import sqlite3
from http.server import SimpleHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

# Add project root to path so we can import our DB logic
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from queuectl.db import get_connection

PORT = 8080

HTML_PAGE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>QueueCTL Dashboard</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #f4f4f9; color: #333; margin: 0; padding: 20px; }
        h1 { border-bottom: 2px solid #333; padding-bottom: 10px; }
        .container { display: flex; gap: 20px; flex-wrap: wrap; }
        .column { flex: 1; min-width: 300px; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
        .metric { font-size: 2em; font-weight: bold; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { text-align: left; padding: 8px; border-bottom: 1px solid #ddd; font-size: 0.9em; }
        th { background-color: #f8f9fa; }
        .badge { padding: 4px 8px; border-radius: 12px; font-size: 0.8em; font-weight: bold; color: white; }
        .status-pending { background-color: #6c757d; }
        .status-processing { background-color: #007bff; }
        .status-completed { background-color: #28a745; }
        .status-failed { background-color: #ffc107; color: #333; }
        .status-dead { background-color: #dc3545; }
        .refresh-note { font-size: 0.8em; color: #888; margin-top: 20px; }
    </style>
    <script>
        // Auto-refresh data every 1 second
        async function fetchData() {
            try {
                const response = await fetch('/api/data');
                const data = await response.json();
                
                // Update metrics
                document.getElementById('m-pending').innerText = data.metrics.pending || 0;
                document.getElementById('m-processing').innerText = data.metrics.processing || 0;
                document.getElementById('m-completed').innerText = data.metrics.completed || 0;
                document.getElementById('m-dead').innerText = data.metrics.dead || 0;

                // Update Workers Table
                const wTbody = document.getElementById('workers-body');
                wTbody.innerHTML = '';
                if (data.workers.length === 0) {
                    wTbody.innerHTML = '<tr><td colspan="4" style="text-align:center; color:#888;">No active workers</td></tr>';
                } else {
                    data.workers.forEach(w => {
                        let row = `<tr>
                            <td>${w.pid}</td>
                            <td>${w.started_at.split('T')[1].replace('Z','')}</td>
                            <td>${w.last_heartbeat.split('T')[1].replace('Z','')}</td>
                            <td>${w.stop_requested ? 'Yes' : 'No'}</td>
                        </tr>`;
                        wTbody.innerHTML += row;
                    });
                }

                // Update Jobs Table
                const jTbody = document.getElementById('jobs-body');
                jTbody.innerHTML = '';
                if (data.jobs.length === 0) {
                    jTbody.innerHTML = '<tr><td colspan="5" style="text-align:center; color:#888;">No jobs found</td></tr>';
                } else {
                    data.jobs.forEach(j => {
                        let badge = `<span class="badge status-${j.state}">${j.state}</span>`;
                        let row = `<tr>
                            <td><strong>${j.id}</strong></td>
                            <td><code>${j.command}</code></td>
                            <td>${badge}</td>
                            <td>${j.attempts}/${j.max_retries}</td>
                            <td>${j.updated_at.split('T')[1].replace('Z','')}</td>
                        </tr>`;
                        jTbody.innerHTML += row;
                    });
                }
            } catch (e) {
                console.error("Failed to fetch data", e);
            }
        }
        setInterval(fetchData, 1000);
        window.onload = fetchData;
    </script>
</head>
<body>
    <h1>🚀 QueueCTL Dashboard</h1>
    
    <div class="container">
        <!-- Metrics -->
        <div class="column">
            <h2>Queue Status</h2>
            <div style="display:flex; justify-content:space-between; text-align:center;">
                <div><div class="metric" id="m-pending" style="color:#6c757d">0</div><div>Pending</div></div>
                <div><div class="metric" id="m-processing" style="color:#007bff">0</div><div>Processing</div></div>
                <div><div class="metric" id="m-completed" style="color:#28a745">0</div><div>Completed</div></div>
                <div><div class="metric" id="m-dead" style="color:#dc3545">0</div><div>DLQ</div></div>
            </div>
        </div>

        <!-- Workers -->
        <div class="column">
            <h2>Active Workers</h2>
            <table>
                <thead><tr><th>PID</th><th>Started</th><th>Heartbeat</th><th>Stop Requested</th></tr></thead>
                <tbody id="workers-body"></tbody>
            </table>
        </div>
    </div>

    <div class="container" style="margin-top: 20px;">
        <!-- Recent Jobs -->
        <div class="column">
            <h2>Recent Jobs</h2>
            <table>
                <thead><tr><th>ID</th><th>Command</th><th>State</th><th>Attempts</th><th>Last Updated</th></tr></thead>
                <tbody id="jobs-body"></tbody>
            </table>
        </div>
    </div>
    
    <div class="refresh-note">Live view • Auto-refreshes every 1s</div>
</body>
</html>
"""

class DashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urlparse(self.path)
        
        # Serve the HTML page
        if parsed_path.path == '/':
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode('utf-8'))
            return
            
        # Serve the JSON API data
        elif parsed_path.path == '/api/data':
            try:
                conn = get_connection()
                
                # Get metrics
                metrics = {"pending": 0, "processing": 0, "completed": 0, "failed": 0, "dead": 0}
                for row in conn.execute("SELECT state, COUNT(*) as count FROM jobs GROUP BY state").fetchall():
                    metrics[row["state"]] = row["count"]
                
                # Get active workers
                workers = [dict(row) for row in conn.execute("SELECT * FROM workers ORDER BY pid").fetchall()]
                
                # Get recent jobs (last 20 updated)
                jobs = [dict(row) for row in conn.execute("SELECT * FROM jobs ORDER BY updated_at DESC LIMIT 20").fetchall()]
                
                conn.close()
                
                data = {
                    "metrics": metrics,
                    "workers": workers,
                    "jobs": jobs
                }
                
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(data).encode('utf-8'))
                
            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode('utf-8'))
            return
            
        # 404 for anything else
        else:
            self.send_response(404)
            self.end_headers()

if __name__ == "__main__":
    server = HTTPServer(('localhost', PORT), DashboardHandler)
    print(f"🌟 Dashboard running at http://localhost:{PORT}")
    print("Press Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down dashboard.")
        sys.exit(0)
