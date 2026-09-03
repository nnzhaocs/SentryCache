"""Minimal memcached client for SentryCache controller.

Raw-socket ASCII protocol; no external deps so we don't have to install
anything else on the controller node. Implements only what the controller's
_prefetch_into needs:

  list_keys()                       — via stats items + stats cachedump
  get_with_meta(key)                — returns (value_bytes, flags) or None
  set_with_meta(key, value, flags, exptime=0)  — preserves flags so the
                                                 caller-side gomemcache lib
                                                 sees byte-identical entries

This file is imported by main.py; for Redis backends we still go through
redis-py. The dispatch happens in `_prefetch_into` based on the deployment's
backend type (set via env or per-target config).
"""
from __future__ import annotations

import socket
from typing import Iterable, Optional, Tuple

DEFAULT_PORT = 11211
RECV_CHUNK = 4096


class MemcachedError(Exception):
    pass


class MemcachedClient:
    def __init__(self, host: str, port: int = DEFAULT_PORT, timeout_s: float = 2.0):
        self.host = host
        self.port = port
        self.timeout_s = timeout_s
        self._sock: Optional[socket.socket] = None

    # -- low-level wire protocol ---------------------------------------

    def _connect(self) -> None:
        if self._sock is not None:
            return
        s = socket.create_connection((self.host, self.port), timeout=self.timeout_s)
        s.settimeout(self.timeout_s)
        self._sock = s

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    def _send(self, data: bytes) -> None:
        self._connect()
        assert self._sock is not None
        self._sock.sendall(data)

    def _recv_until(self, terminator: bytes) -> bytes:
        """Read from socket until `terminator` substring appears at end of any
        line of accumulated bytes. Returns the full accumulated buffer."""
        self._connect()
        assert self._sock is not None
        buf = b""
        while True:
            chunk = self._sock.recv(RECV_CHUNK)
            if not chunk:
                raise MemcachedError("connection closed mid-read")
            buf += chunk
            if terminator in buf:
                return buf

    # -- public API ----------------------------------------------------

    def stats_items(self) -> dict[int, int]:
        """Returns {slab_class_id: number_of_items}."""
        self._send(b"stats items\r\n")
        buf = self._recv_until(b"END\r\n")
        result: dict[int, int] = {}
        for line in buf.split(b"\r\n"):
            # STAT items:<slab>:number <count>
            if line.startswith(b"STAT items:") and b":number" in line:
                parts = line.split()
                if len(parts) != 3:
                    continue
                try:
                    slab = int(parts[1].split(b":")[1])
                    n = int(parts[2])
                except (IndexError, ValueError):
                    continue
                result[slab] = n
        return result

    def cachedump_slab(self, slab: int, limit: int = 200) -> list[Tuple[bytes, int]]:
        """Returns [(key_bytes, byte_size)] for at most `limit` items in slab.

        Note: `cachedump` is documented as a debug/sampling tool and may not
        return every key for high-traffic memcacheds. It returns recently
        accessed items, which is good enough for SentryCache's "prefetch hot
        keys" — we treat the cachedump output as a proxy for "approximately
        active keys."
        """
        cmd = f"stats cachedump {slab} {limit}\r\n".encode()
        self._send(cmd)
        buf = self._recv_until(b"END\r\n")
        items: list[Tuple[bytes, int]] = []
        for line in buf.split(b"\r\n"):
            # ITEM <key> [<size> b; <expires> s]
            if not line.startswith(b"ITEM "):
                continue
            try:
                # split into ITEM, key, [size, b;, expires, s]
                rest = line[5:]
                space = rest.find(b" [")
                if space < 0:
                    continue
                key = rest[:space]
                meta = rest[space + 2:].rstrip(b"]").rstrip()
                # meta = "<size> b; <expires> s"
                parts = meta.split(b";")
                size_part = parts[0].strip().split()
                size = int(size_part[0])
                items.append((key, size))
            except Exception:
                continue
        return items

    def list_keys_with_size(self, max_keys: int = 5000) -> list[Tuple[bytes, int]]:
        """Returns [(key, size)] sampled across all slabs, up to max_keys."""
        out: list[Tuple[bytes, int]] = []
        slabs = self.stats_items()
        for slab in sorted(slabs):
            limit = min(slabs[slab], max(50, max_keys - len(out)))
            if limit <= 0:
                break
            try:
                items = self.cachedump_slab(slab, limit)
                out.extend(items)
            except Exception:
                continue
            if len(out) >= max_keys:
                break
        return out[:max_keys]

    def get_with_meta(self, key: bytes) -> Optional[Tuple[bytes, int]]:
        """gets <key> — returns (value, flags) or None on miss."""
        if b" " in key or b"\r" in key or b"\n" in key:
            return None  # invalid key format
        cmd = b"get " + key + b"\r\n"
        self._send(cmd)
        buf = self._recv_until(b"END\r\n")
        # Parse: VALUE <key> <flags> <bytes>\r\n<data>\r\nEND\r\n  (or just END\r\n)
        first_nl = buf.find(b"\r\n")
        if first_nl < 0:
            return None
        first = buf[:first_nl]
        if not first.startswith(b"VALUE "):
            return None  # miss
        parts = first.split()
        if len(parts) < 4:
            return None
        try:
            flags = int(parts[2])
            n = int(parts[3])
        except ValueError:
            return None
        data_start = first_nl + 2
        value = buf[data_start:data_start + n]
        return value, flags

    def set_with_meta(self, key: bytes, value: bytes, flags: int = 0, exptime: int = 0) -> bool:
        if b" " in key or b"\r" in key or b"\n" in key:
            return False
        header = f"set ".encode() + key + f" {flags} {exptime} {len(value)}\r\n".encode()
        self._send(header + value + b"\r\n")
        buf = self._recv_until(b"\r\n")
        return buf.startswith(b"STORED")

    def stats(self) -> dict[bytes, bytes]:
        self._send(b"stats\r\n")
        buf = self._recv_until(b"END\r\n")
        result = {}
        for line in buf.split(b"\r\n"):
            if line.startswith(b"STAT "):
                parts = line[5:].split(b" ", 1)
                if len(parts) == 2:
                    result[parts[0]] = parts[1]
        return result

    def ping(self) -> bool:
        try:
            self.stats()
            return True
        except Exception:
            return False
