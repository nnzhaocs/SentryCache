"""SentryCache RESP-level sidecar proxy (Phase 13.2 — L2-integrated).

Topology:

    application container   ──TCP──▶  proxy LISTEN_PORT (this process)
                                          │  rewrite + register L2 + peer-fetch
                                          ▼
                                      upstream Redis sidecar (UPSTREAM_REDIS_PORT)
    peer proxies             ──TCP──▶  proxy PEER_FETCH_PORT (this process)
                                          │  plain forward
                                          ▼
                                      upstream Redis sidecar (UPSTREAM_REDIS_PORT)

The application speaks unmodified RESP2 to localhost:LISTEN_PORT. Proxy parses
each command, prefixes its key argument(s) with VERSION_TAG, forwards to the
upstream, and (if L2 is configured) maintains the replica index in the L2
metadata store. On a read miss the proxy queries L2 for siblings holding the
key and pulls it via the peer's PEER_FETCH_PORT.

Per-pod identity is supplied by the Kubernetes Downward API (POD_NAME / POD_IP
/ NODE_NAME).  Without an L2 endpoint the proxy degrades to the Phase 13.0b
behaviour: prefix only, no replica registration, no peer fetch.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from typing import List, Optional, Tuple

LOG = logging.getLogger("sentrycache.proxy")

# ---------- configuration -----------------------------------------------------

LISTEN_HOST: str = os.environ.get("LISTEN_HOST", "0.0.0.0")
LISTEN_PORT: int = int(os.environ.get("LISTEN_PORT", "6379"))
PEER_FETCH_PORT: int = int(os.environ.get("PEER_FETCH_PORT", "6378"))
UPSTREAM_HOST: str = os.environ.get("UPSTREAM_REDIS_HOST", "127.0.0.1")
UPSTREAM_PORT: int = int(os.environ.get("UPSTREAM_REDIS_PORT", "6380"))
VERSION_TAG: bytes = os.environ.get("VERSION_TAG", "v_default").encode()

POD_NAME: str = os.environ.get("POD_NAME", "anon")
POD_IP: str = os.environ.get("POD_IP", "")
NODE_NAME: str = os.environ.get("NODE_NAME", "unknown")

L2_INDEX_HOST: str = os.environ.get("L2_INDEX_HOST", "")
L2_INDEX_PORT: int = int(os.environ.get("L2_INDEX_PORT", "6379"))

PREFIX_DELIM: bytes = b":"
PEER_FETCH_TIMEOUT_S: float = float(os.environ.get("PEER_FETCH_TIMEOUT_S", "1.5"))

# ---------- command classification -------------------------------------------

# Single-key commands whose key is at argv[1].
SINGLE_KEY: frozenset = frozenset([
    b"SET", b"GET", b"GETSET", b"SETNX", b"SETEX", b"PSETEX", b"GETEX", b"GETDEL",
    b"INCR", b"INCRBY", b"INCRBYFLOAT", b"DECR", b"DECRBY",
    b"APPEND", b"STRLEN", b"GETRANGE", b"SETRANGE",
    b"EXPIRE", b"EXPIREAT", b"PEXPIRE", b"PEXPIREAT",
    b"TTL", b"PTTL", b"PERSIST", b"TYPE", b"DUMP", b"RESTORE",
    b"HGET", b"HSET", b"HDEL", b"HEXISTS", b"HKEYS", b"HVALS",
    b"HGETALL", b"HLEN", b"HINCRBY", b"HINCRBYFLOAT", b"HMGET", b"HMSET",
    b"HSCAN", b"HSETNX",
    b"LPUSH", b"RPUSH", b"LPOP", b"RPOP", b"LRANGE", b"LLEN",
    b"LINDEX", b"LSET", b"LREM", b"LINSERT", b"LTRIM", b"LPUSHX", b"RPUSHX",
    b"SADD", b"SREM", b"SMEMBERS", b"SISMEMBER", b"SCARD", b"SPOP",
    b"SRANDMEMBER", b"SSCAN",
    b"ZADD", b"ZREM", b"ZRANGE", b"ZREVRANGE", b"ZRANGEBYSCORE",
    b"ZREVRANGEBYSCORE", b"ZRANK", b"ZREVRANK", b"ZSCORE", b"ZINCRBY", b"ZCARD",
    b"ZCOUNT", b"ZSCAN", b"ZLEXCOUNT", b"ZRANGEBYLEX", b"ZREMRANGEBYRANK",
    b"ZREMRANGEBYSCORE", b"ZPOPMIN", b"ZPOPMAX",
    b"GETBIT", b"SETBIT", b"BITCOUNT", b"BITPOS",
])

OBJECT_LIKE: frozenset = frozenset([b"OBJECT", b"PFCOUNT"])
MULTI_KEY_ALL: frozenset = frozenset([b"DEL", b"EXISTS", b"MGET", b"UNLINK", b"TOUCH"])
MSET_STYLE: frozenset = frozenset([b"MSET", b"MSETNX"])
PAIR_KEY: frozenset = frozenset([b"RENAME", b"RENAMENX", b"COPY", b"SMOVE", b"LMOVE", b"BLMOVE"])

PASSTHROUGH: frozenset = frozenset([
    b"PING", b"ECHO", b"INFO", b"COMMAND", b"CLUSTER", b"CLIENT", b"CONFIG",
    b"DEBUG", b"SELECT", b"AUTH", b"HELLO", b"QUIT", b"FLUSHDB", b"FLUSHALL",
    b"DBSIZE", b"SCRIPT", b"EVAL", b"EVALSHA", b"WAIT", b"MULTI", b"EXEC",
    b"DISCARD", b"WATCH", b"UNWATCH", b"PUBLISH", b"MONITOR",
    b"ROLE", b"LATENCY", b"MEMORY", b"TIME", b"LASTSAVE", b"SAVE", b"BGSAVE",
    b"BGREWRITEAOF", b"SLOWLOG", b"REPLICAOF", b"SLAVEOF", b"SHUTDOWN",
    b"KEYS", b"SCAN", b"RANDOMKEY", b"RESET", b"SUBSCRIBE", b"UNSUBSCRIBE",
    b"PSUBSCRIBE", b"PUNSUBSCRIBE", b"READONLY", b"READWRITE",
    b"FUNCTION", b"ACL",
])

# Commands whose successful execution populates a key — register self into L2 Sk.
WRITE_REGISTER_CMDS: frozenset = frozenset([
    b"SET", b"GETSET", b"SETNX", b"SETEX", b"PSETEX", b"GETEX",
    b"INCR", b"INCRBY", b"INCRBYFLOAT", b"DECR", b"DECRBY",
    b"APPEND", b"SETRANGE", b"SETBIT",
    b"HSET", b"HSETNX", b"HMSET", b"HINCRBY", b"HINCRBYFLOAT",
    b"LPUSH", b"RPUSH", b"LPUSHX", b"RPUSHX", b"LSET", b"LINSERT",
    b"SADD",
    b"ZADD", b"ZINCRBY",
    b"RESTORE", b"COPY",
])

# Commands whose nil reply means cache miss → eligible for peer-fetch.
READ_MISS_CMDS: frozenset = frozenset([
    b"GET", b"HGET", b"HGETALL", b"HMGET",
    b"LINDEX", b"LRANGE",
    b"ZSCORE", b"ZRANGE", b"ZREVRANGE", b"ZRANGEBYSCORE",
    b"DUMP",
])


def prefix(key: bytes) -> bytes:
    return VERSION_TAG + PREFIX_DELIM + key


# ---------- RESP parser / encoder --------------------------------------------

async def read_resp(reader: asyncio.StreamReader) -> Tuple[Optional[bytes], object]:
    line = await reader.readline()
    if not line or not line.endswith(b"\r\n"):
        return None, None
    raw = line
    head = line[:1]
    body = line[1:-2]

    if head == b"*":
        n = int(body)
        if n < 0:
            return raw, None
        items = []
        for _ in range(n):
            sub_raw, sub_val = await read_resp(reader)
            if sub_raw is None:
                return None, None
            raw += sub_raw
            items.append(sub_val)
        return raw, items

    if head == b"$":
        n = int(body)
        if n < 0:
            return raw, None
        data = await reader.readexactly(n)
        crlf = await reader.readexactly(2)
        return raw + data + crlf, data

    if head in (b"+", b"-", b":"):
        return raw, body

    parts = body.split(b" ")
    return raw, parts


def encode_array(items: List[bytes]) -> bytes:
    out = [f"*{len(items)}\r\n".encode()]
    for it in items:
        out.append(f"${len(it)}\r\n".encode())
        out.append(it)
        out.append(b"\r\n")
    return b"".join(out)


def encode_bulk(value: Optional[bytes]) -> bytes:
    if value is None:
        return b"$-1\r\n"
    return f"${len(value)}\r\n".encode() + value + b"\r\n"


# ---------- command rewriting ------------------------------------------------

def rewrite_command(cmd_array: List[bytes]) -> Optional[List[bytes]]:
    if not cmd_array:
        return None
    head = cmd_array[0]
    if not isinstance(head, (bytes, bytearray)):
        return None
    cmd = bytes(head).upper()

    if cmd in PASSTHROUGH:
        return None

    if cmd in SINGLE_KEY:
        if len(cmd_array) >= 2 and isinstance(cmd_array[1], (bytes, bytearray)):
            return [cmd_array[0], prefix(cmd_array[1])] + list(cmd_array[2:])
        return None

    if cmd in OBJECT_LIKE:
        if len(cmd_array) >= 3 and isinstance(cmd_array[2], (bytes, bytearray)):
            return [cmd_array[0], cmd_array[1], prefix(cmd_array[2])] + list(cmd_array[3:])
        return None

    if cmd in MULTI_KEY_ALL:
        if len(cmd_array) < 2:
            return None
        return [cmd_array[0]] + [prefix(k) for k in cmd_array[1:]]

    if cmd in MSET_STYLE:
        new = [cmd_array[0]]
        for i, x in enumerate(cmd_array[1:]):
            if i % 2 == 0:
                new.append(prefix(x))
            else:
                new.append(x)
        return new

    if cmd in PAIR_KEY:
        if len(cmd_array) >= 3:
            return [cmd_array[0], prefix(cmd_array[1]), prefix(cmd_array[2])] + list(cmd_array[3:])
        return None

    return None


# ---------- L2 client (lazy, async, fail-soft) -------------------------------

class L2Client:
    """Thin async wrapper over redis.asyncio. Self-register on init; expose
    HSET / HGETALL / HGET helpers used by the proxy's hot path. All errors are
    swallowed and counted — L2 is best-effort, the application path must not
    fail when L2 is down."""

    def __init__(self, host: str, port: int):
        import redis.asyncio as redis_async
        self._redis = redis_async.Redis(
            host=host, port=port, decode_responses=False, socket_timeout=2.0
        )
        self.errors = 0

    async def ping(self) -> bool:
        try:
            return bool(await self._redis.ping())
        except Exception as e:
            self.errors += 1
            LOG.warning("L2 ping failed: %s", e)
            return False

    async def self_register(self):
        try:
            ep = f"{POD_IP}:{PEER_FETCH_PORT}".encode() if POD_IP else b""
            await self._redis.hset(b"nodemap", POD_NAME.encode(), ep)
            await self._redis.hset(b"node_zone", POD_NAME.encode(), NODE_NAME.encode())
            await self._redis.hset(b"versionmap", POD_NAME.encode(), VERSION_TAG)
            LOG.info("L2 self-register pod=%s ip=%s node=%s ver=%s",
                     POD_NAME, POD_IP, NODE_NAME, VERSION_TAG.decode())
        except Exception as e:
            self.errors += 1
            LOG.warning("L2 self_register failed: %s", e)

    async def register_write(self, cache_key: bytes):
        try:
            ts = str(int(time.time())).encode()
            await self._redis.hset(
                b"replicas:" + VERSION_TAG + b":" + cache_key,
                POD_NAME.encode(),
                ts,
            )
        except Exception as e:
            self.errors += 1
            LOG.debug("L2 register_write failed: %s", e)

    async def get_peers(self, cache_key: bytes) -> List[bytes]:
        try:
            sk = await self._redis.hgetall(
                b"replicas:" + VERSION_TAG + b":" + cache_key
            )
        except Exception as e:
            self.errors += 1
            LOG.debug("L2 get_peers failed: %s", e)
            return []
        return [k for k in sk.keys() if k != POD_NAME.encode()]

    async def get_endpoint(self, peer_pod: bytes) -> Optional[str]:
        try:
            ep = await self._redis.hget(b"nodemap", peer_pod)
        except Exception as e:
            self.errors += 1
            LOG.debug("L2 get_endpoint failed: %s", e)
            return None
        if not ep:
            return None
        return ep.decode() if isinstance(ep, (bytes, bytearray)) else ep


# ---------- peer fetch -------------------------------------------------------

async def peer_fetch_value(l2: L2Client, cache_key: bytes,
                           prefixed_key: bytes, original_cmd: bytes) -> Optional[bytes]:
    """Try to pull `prefixed_key` from a sibling pod. Returns the bulk value
    on success, None on miss / failure. Fails soft on every error path."""
    peers = await l2.get_peers(cache_key)
    if not peers:
        return None

    random.shuffle(peers)
    for peer in peers[:3]:  # try at most 3 peers
        ep = await l2.get_endpoint(peer)
        if not ep or ":" not in ep:
            continue
        host, _, port = ep.partition(":")
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, int(port)),
                timeout=PEER_FETCH_TIMEOUT_S,
            )
        except Exception as e:
            LOG.debug("peer connect %s failed: %s", ep, e)
            continue
        try:
            cmd = encode_array([original_cmd, prefixed_key])
            writer.write(cmd)
            await writer.drain()
            _, parsed = await asyncio.wait_for(
                read_resp(reader), timeout=PEER_FETCH_TIMEOUT_S
            )
            if parsed is not None and not isinstance(parsed, list):
                # bulk-string hit
                return parsed
        except Exception as e:
            LOG.debug("peer fetch from %s failed: %s", ep, e)
        finally:
            try:
                writer.close()
            except Exception:
                pass
    return None


# ---------- app-side connection handler --------------------------------------

async def handle_app_connection(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    l2: Optional[L2Client],
) -> None:
    """One coroutine per app connection. Runs a serial request/response loop
    against the upstream Redis sidecar so we can intercept replies (needed for
    write-completion L2 registration and read-miss peer-fetch)."""
    peer = client_writer.get_extra_info("peername")
    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(
            UPSTREAM_HOST, UPSTREAM_PORT
        )
    except OSError as e:
        LOG.error("upstream connect failed (app conn from %s): %s", peer, e)
        try:
            client_writer.close()
        except Exception:
            pass
        return

    try:
        while True:
            req_raw, parsed = await read_resp(client_reader)
            if req_raw is None:
                break

            # Multi-bulk command → classify, rewrite, instrument
            if (isinstance(parsed, list) and parsed
                    and all(isinstance(x, (bytes, bytearray)) for x in parsed)):
                cmd_name = bytes(parsed[0]).upper()
                argv = parsed
                new_argv = rewrite_command(argv)
                if new_argv is None:
                    upstream_writer.write(req_raw)
                else:
                    upstream_writer.write(encode_array(new_argv))
                await upstream_writer.drain()

                reply_raw, reply_parsed = await read_resp(upstream_reader)
                if reply_raw is None:
                    break

                # Fire-and-forget L2 registration on successful writes.
                if (l2 is not None
                        and cmd_name in WRITE_REGISTER_CMDS
                        and len(argv) >= 2
                        and not reply_raw.startswith(b"-")):
                    asyncio.create_task(l2.register_write(argv[1]))

                # Read-miss path: try peer fetch on nil bulk reply.
                replaced_reply: Optional[bytes] = None
                if (l2 is not None
                        and cmd_name in READ_MISS_CMDS
                        and len(argv) >= 2
                        and reply_raw == b"$-1\r\n"):
                    cache_key = argv[1]
                    prefixed = prefix(cache_key)
                    value = await peer_fetch_value(l2, cache_key, prefixed, cmd_name)
                    if value is not None:
                        # Writeback into local upstream so future GETs hit.
                        wb = encode_array([b"SET", prefixed, value])
                        upstream_writer.write(wb)
                        await upstream_writer.drain()
                        await read_resp(upstream_reader)  # consume +OK
                        asyncio.create_task(l2.register_write(cache_key))
                        replaced_reply = encode_bulk(value)

                client_writer.write(replaced_reply if replaced_reply else reply_raw)
                await client_writer.drain()
            else:
                # Inline / unknown framing → pass through with one round trip.
                upstream_writer.write(req_raw)
                await upstream_writer.drain()
                reply_raw, _ = await read_resp(upstream_reader)
                if reply_raw is None:
                    break
                client_writer.write(reply_raw)
                await client_writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError) as e:
        LOG.debug("app conn from %s closed: %s", peer, e)
    finally:
        for w in (client_writer, upstream_writer):
            try:
                w.close()
            except Exception:
                pass


# ---------- peer-side connection handler -------------------------------------

async def handle_peer_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    """A peer-fetch port connection is just a transparent forwarder to the
    upstream Redis sidecar. No L2 lookup, no prefix rewriting (caller already
    sent the prefixed key). This avoids peer-fetch loops."""
    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(
            UPSTREAM_HOST, UPSTREAM_PORT
        )
    except OSError as e:
        LOG.error("peer-fetch upstream connect failed: %s", e)
        try:
            writer.close()
        except Exception:
            pass
        return

    async def c2u():
        while True:
            try:
                data = await reader.read(65536)
            except Exception:
                return
            if not data:
                return
            upstream_writer.write(data)
            try:
                await upstream_writer.drain()
            except Exception:
                return

    async def u2c():
        while True:
            try:
                data = await upstream_reader.read(65536)
            except Exception:
                return
            if not data:
                return
            writer.write(data)
            try:
                await writer.drain()
            except Exception:
                return

    try:
        await asyncio.gather(c2u(), u2c(), return_exceptions=True)
    finally:
        for w in (writer, upstream_writer):
            try:
                w.close()
            except Exception:
                pass


# ---------- main -------------------------------------------------------------

async def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    l2: Optional[L2Client] = None
    if L2_INDEX_HOST:
        l2 = L2Client(L2_INDEX_HOST, L2_INDEX_PORT)
        if await l2.ping():
            await l2.self_register()
        else:
            LOG.warning("L2 unreachable at boot; running degraded (no peer-fetch)")
            l2 = None
    else:
        LOG.info("L2_INDEX_HOST not set; running standalone (no L2 / no peer-fetch)")

    app_server = await asyncio.start_server(
        lambda r, w: handle_app_connection(r, w, l2),
        LISTEN_HOST, LISTEN_PORT,
    )
    peer_server = await asyncio.start_server(
        handle_peer_connection,
        LISTEN_HOST, PEER_FETCH_PORT,
    )

    app_socks = ", ".join(str(s.getsockname()) for s in (app_server.sockets or []))
    peer_socks = ", ".join(str(s.getsockname()) for s in (peer_server.sockets or []))
    LOG.info(
        "proxy up app=%s peer=%s upstream=%s:%s ver=%s pod=%s/%s/%s l2=%s",
        app_socks, peer_socks, UPSTREAM_HOST, UPSTREAM_PORT,
        VERSION_TAG.decode(), POD_NAME, POD_IP, NODE_NAME,
        f"{L2_INDEX_HOST}:{L2_INDEX_PORT}" if L2_INDEX_HOST else "off",
    )

    try:
        await asyncio.gather(
            app_server.serve_forever(),
            peer_server.serve_forever(),
        )
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    if os.environ.get("USE_UVLOOP", "0") == "1":
        try:
            import uvloop
            uvloop.install()
        except ImportError:
            LOG.warning("uvloop requested but not installed; falling back to default asyncio")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
