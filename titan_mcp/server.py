"""Dependency-free MCP stdio server for Titan.

Stdout is reserved exclusively for JSON-RPC messages. Diagnostics go to stderr.
"""

from __future__ import annotations

import json
import logging
import sys
from contextlib import redirect_stdout
from typing import Any, TextIO

from . import __version__
from .config import McpConfig
from .gateway import TitanGateway
from .tools import ToolCatalog, tool_error, tool_result


LOG = logging.getLogger("titan_mcp")
SUPPORTED_PROTOCOLS = ("2025-11-25", "2025-06-18", "2025-03-26")


class TitanMcpServer:
    def __init__(self, catalog: ToolCatalog):
        self.catalog = catalog
        self.initialize_seen = False
        self.initialized = False

    def handle(self, message: Any) -> dict[str, Any] | None:
        if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
            return self._error(message.get("id") if isinstance(message, dict) else None, -32600, "Invalid Request")
        has_id = "id" in message
        request_id = message.get("id")
        method = message.get("method")
        params = message.get("params", {})
        if params is None:
            params = {}
        if not isinstance(params, dict):
            return self._error(request_id, -32602, "params must be an object") if has_id else None
        if not has_id:
            if method == "notifications/initialized":
                if self.initialize_seen:
                    self.initialized = True
            return None
        try:
            if method == "initialize":
                if self.initialize_seen:
                    return self._error(request_id, -32600, "Server is already initialized")
                self.initialize_seen = True
                requested = params.get("protocolVersion")
                protocol = requested if requested in SUPPORTED_PROTOCOLS else SUPPORTED_PROTOCOLS[0]
                return self._result(request_id, {
                    "protocolVersion": protocol,
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "titan-mcp", "version": __version__},
                    "instructions": "Inspect Titan before mutating it. Write tools require TITAN_MCP_OPERATOR=true.",
                })
            if method == "ping":
                return self._result(request_id, {})
            if not self.initialized:
                return self._error(request_id, -32002, "Server initialization is not complete")
            if method == "tools/list":
                return self._result(request_id, {"tools": self.catalog.definitions()})
            if method == "tools/call":
                name = params.get("name")
                if not isinstance(name, str):
                    raise ValueError("tools/call requires a tool name")
                try:
                    with redirect_stdout(sys.stderr):
                        value = self.catalog.call(name, params.get("arguments"))
                    return self._result(request_id, tool_result(value))
                except Exception as error:  # A tool failure is data, not a server crash.
                    LOG.warning("Tool %s failed: %s", name, error)
                    return self._result(request_id, tool_error(error))
            return self._error(request_id, -32601, "Method not found")
        except (TypeError, ValueError) as error:
            return self._error(request_id, -32602, str(error))
        except Exception:
            LOG.exception("Unhandled MCP request failure")
            return self._error(request_id, -32603, "Internal error")

    @staticmethod
    def _result(request_id: Any, result: Any) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": request_id, "result": result}

    @staticmethod
    def _error(request_id: Any, code: int, message: str) -> dict[str, Any]:
        return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def serve(stdin: TextIO = sys.stdin, stdout: TextIO = sys.stdout) -> int:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="[%(name)s] %(levelname)s %(message)s")
    config = McpConfig.from_env()
    server = TitanMcpServer(ToolCatalog(TitanGateway(config), config))
    for line in stdin:
        try:
            message = json.loads(line)
            response = server.handle(message)
        except json.JSONDecodeError:
            response = server._error(None, -32700, "Parse error")
        if response is not None:
            stdout.write(json.dumps(response, separators=(",", ":")) + "\n")
            stdout.flush()
    return 0


def main() -> int:
    return serve()


if __name__ == "__main__":
    raise SystemExit(main())
