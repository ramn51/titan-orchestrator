
from http.server import BaseHTTPRequestHandler, HTTPServer
import sys

PORT = 9999

class SimpleHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Titan Service is ALIVE!")
        print(f"[SERVER] Handled Request from {self.client_address}")

print(f"[SERVER] Starting Agent on port {PORT}...")
# Flush stdout is critical for Titan logs!
sys.stdout.flush() 

httpd = HTTPServer(('0.0.0.0', PORT), SimpleHandler)
httpd.serve_forever()
