import sys
from flask import Flask, jsonify

app = Flask(__name__)

# The port is passed as an argument by Titan when it runs the script
# e.g., python server.py 5000
port = int(sys.argv[1]) if len(sys.argv) > 1 else 5000

@app.route('/')
def home():
    return jsonify({
        "status": "ONLINE",
        "message": "Hello from Titan Orchestrator!",
        "service": "Python-Microservice"
    })

@app.route('/ping')
def ping():
    return "pong"

if __name__ == '__main__':
    print(f"[*] Starting Microservice on port {port}...")
    app.run(host='0.0.0.0', port=port)