#!/usr/bin/env python3
"""Minimal HTTP server that prints POST bodies to stdout.

Usage: python3 mlog_echo_server.py [port]
Default port: 9999
"""

import sys
import json
from http.server import HTTPServer, BaseHTTPRequestHandler


class EchoHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        # Print headers of interest
        print(f"\n{'=' * 60}")
        print(f"POST {self.path}")
        print(f"Content-Type: {self.headers.get('Content-Type', '?')}")
        print(f"Content-Length: {content_length}")
        for key, value in self.headers.items():
            if key.lower() not in (
                "host",
                "user-agent",
                "accept",
                "accept-encoding",
                "content-type",
                "content-length",
            ):
                print(f"{key}: {value}")

        # Parse and display body based on content type
        content_type = self.headers.get("Content-Type", "")
        if content_type == "application/json":
            try:
                data = json.loads(body)
                event_count = len(data) if isinstance(data, list) else 1
                print(f"Events in batch: {event_count}")
                print(json.dumps(data, indent=2)[:4000])
            except json.JSONDecodeError:
                print(f"Raw body: {body[:2000]}")
        elif content_type == "application/x-ndjson":
            lines = body.decode("utf-8", errors="replace").strip().split("\n")
            print(f"Events in batch (NDJSON): {len(lines)}")
            for i, line in enumerate(lines[:10]):
                try:
                    obj = json.loads(line)
                    print(f"  [{i}] {json.dumps(obj)[:200]}")
                except json.JSONDecodeError:
                    print(f"  [{i}] (parse error) {line[:200]}")
        elif content_type in ("application/qlog+json-seq", "application/json-seq"):
            # Split on Record Separator (0x1e), skip empty first element
            records = body.split(b"\x1e")
            records = [r.strip() for r in records if r.strip()]
            print(f"Events in batch (JSON-SEQ): {len(records)}")
            for i, record in enumerate(records[:10]):
                try:
                    obj = json.loads(record)
                    print(f"  [{i}] \\x1e{json.dumps(obj)[:200]}")
                except json.JSONDecodeError:
                    print(f"  [{i}] (parse error) {record[:200]}")
        else:
            print(f"Raw body ({len(body)} bytes): {body[:2000]}")

        print(f"{'=' * 60}")
        sys.stdout.flush()

        # Respond 200 OK
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"ok\n")

    def log_message(self, format, *args):
        # Suppress default access log — we have our own output
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9999
    server = HTTPServer(("127.0.0.1", port), EchoHandler)
    print(f"mlog echo server listening on http://127.0.0.1:{port}")
    print("Waiting for POST requests...")
    sys.stdout.flush()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
