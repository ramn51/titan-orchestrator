from http.server import BaseHTTPRequestHandler, HTTPServer
import sys

PORT = 9000

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.wfile.write(b"Titan Comprehensive Test: SUCCESS")

print(f"[SERVER] Starting Test Server on {PORT}...")
sys.stdout.flush()

httpd = HTTPServer(('0.0.0.0', PORT), Handler)
httpd.serve_forever()