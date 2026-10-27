"""Runtime configuration for the Titan MCP sidecar."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _boolean(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class McpConfig:
    host: str
    port: int
    operator_mode: bool
    allowed_roots: tuple[Path, ...]
    kv_prefix: str
    max_script_bytes: int
    max_asset_bytes: int

    @classmethod
    def from_env(cls) -> "McpConfig":
        roots_value = os.environ.get("TITAN_MCP_ALLOWED_ROOTS", os.getcwd())
        roots = tuple(Path(item).expanduser().resolve() for item in roots_value.split(os.pathsep) if item)
        if not roots:
            raise ValueError("TITAN_MCP_ALLOWED_ROOTS must contain at least one directory")
        port = int(os.environ.get("TITAN_PORT", "9090"))
        if not 1 <= port <= 65535:
            raise ValueError("TITAN_PORT must be from 1 to 65535")
        prefix = os.environ.get("TITAN_MCP_KV_PREFIX", "user:mcp:")
        if not prefix or "|" in prefix or len(prefix) > 128:
            raise ValueError("TITAN_MCP_KV_PREFIX must contain 1-128 characters without '|'")
        if not prefix.startswith("user:"):
            raise ValueError("TITAN_MCP_KV_PREFIX must remain inside the user: namespace")
        return cls(
            host=os.environ.get("TITAN_HOST", "127.0.0.1"),
            port=port,
            operator_mode=_boolean("TITAN_MCP_OPERATOR", False),
            allowed_roots=roots,
            kv_prefix=prefix,
            max_script_bytes=int(os.environ.get("TITAN_MCP_MAX_SCRIPT_BYTES", str(6 * 1024 * 1024))),
            max_asset_bytes=int(os.environ.get("TITAN_MCP_MAX_ASSET_BYTES", str(6 * 1024 * 1024))),
        )

    def resolve_read_path(self, raw_path: str, maximum_bytes: int) -> Path:
        candidate = Path(raw_path).expanduser().resolve(strict=True)
        if not candidate.is_file():
            raise ValueError(f"Not a regular file: {raw_path}")
        if not any(candidate == root or root in candidate.parents for root in self.allowed_roots):
            raise PermissionError("Path is outside TITAN_MCP_ALLOWED_ROOTS")
        size = candidate.stat().st_size
        if size > maximum_bytes:
            raise ValueError(f"File is {size} bytes; configured limit is {maximum_bytes}")
        return candidate

    def resolve_write_path(self, raw_path: str) -> Path:
        candidate = Path(raw_path).expanduser().resolve(strict=False)
        parent = candidate.parent.resolve(strict=True)
        if not any(parent == root or root in parent.parents for root in self.allowed_roots):
            raise PermissionError("Destination is outside TITAN_MCP_ALLOWED_ROOTS")
        return candidate

    def namespaced_key(self, key: str) -> str:
        value = key.strip()
        if not value or len(value) > 256 or "|" in value:
            raise ValueError("KV key must contain 1-256 characters without '|'")
        return self.kv_prefix + value
