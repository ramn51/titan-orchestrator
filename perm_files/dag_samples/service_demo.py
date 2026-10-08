"""Small local HTTP service for demonstrating Titan's readiness-gated DAG deploy."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = b"Titan service demo is ready\n"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    print("Service demo listening on 8123", flush=True)
    ThreadingHTTPServer(("0.0.0.0", 8123), Handler).serve_forever()
