import json
import socket
import struct
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from titan_mcp.config import McpConfig
from titan_mcp.gateway import OP_STATS_JSON, TitanGateway


class OneShotTitanServer:
    def __init__(self, response, chunks=False):
        self.response = response.encode("utf-8")
        self.chunks = chunks
        self.request = None
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(1)
        self.port = self.listener.getsockname()[1]
        self.thread = threading.Thread(target=self._run, daemon=True)

    @staticmethod
    def _read_exact(connection, length):
        data = bytearray()
        while len(data) < length:
            chunk = connection.recv(length - len(data))
            if not chunk:
                raise ConnectionError("client disconnected")
            data.extend(chunk)
        return bytes(data)

    def _run(self):
        try:
            connection, _ = self.listener.accept()
            with connection:
                header = self._read_exact(connection, 8)
                version, opcode, _, _, length = struct.unpack(">BBBBI", header)
                payload = self._read_exact(connection, length).decode("utf-8")
                self.request = (version, opcode, payload)
                response = struct.pack(">BBBBI", 1, 0x52, 0, 0, len(self.response)) + self.response
                if self.chunks:
                    for byte in response:
                        connection.sendall(bytes([byte]))
                else:
                    connection.sendall(response)
        finally:
            self.listener.close()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.thread.join(timeout=3)


class GatewayProtocolTests(unittest.TestCase):
    def config(self, port):
        return McpConfig("127.0.0.1", port, False, (Path.cwd(),), "user:mcp:", 1000, 1000)

    def test_stats_uses_existing_opcode_and_projection(self):
        with OneShotTitanServer(json.dumps({"counts": {"ready": 2}}), chunks=True) as server:
            result = TitanGateway(self.config(server.port)).stats("queue|25")
        self.assertEqual({"ready": 2}, result["counts"])
        self.assertEqual((1, OP_STATS_JSON, "queue|25"), server.request)

    def test_timeline_accepts_the_existing_array_contract(self):
        spans = [{"job_id": "DAG-a", "status": "COMPLETED"}]
        with OneShotTitanServer(json.dumps(spans), chunks=True) as server:
            result = TitanGateway(self.config(server.port)).stats("timeline|0|10")
        self.assertEqual(spans, result)
        self.assertEqual((1, OP_STATS_JSON, "timeline|0|10"), server.request)

    def test_malformed_stats_is_bounded_error(self):
        with OneShotTitanServer("not-json") as server:
            with self.assertRaisesRegex(RuntimeError, "malformed stats JSON"):
                TitanGateway(self.config(server.port)).stats("system")

    def test_titan_error_response_is_not_treated_as_success(self):
        with OneShotTitanServer("ERROR: unavailable") as server:
            with self.assertRaisesRegex(RuntimeError, "unavailable"):
                TitanGateway(self.config(server.port)).stats("system")

    def test_unreachable_master_returns_bounded_error(self):
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        listener.close()
        with self.assertRaisesRegex(RuntimeError, "CONNECTION_ERROR"):
            TitanGateway(self.config(port)).stats("system")


if __name__ == "__main__":
    unittest.main()
