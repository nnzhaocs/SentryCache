// SentryCache RESP-level sidecar proxy (Go reimplementation).
//
// Topology mirrors the Python prototype:
//
//   application container ──▶ proxy LISTEN_PORT ──▶ upstream Redis (UPSTREAM_REDIS_PORT)
//   peer proxies          ──▶ proxy PEER_FETCH_PORT ──▶ upstream Redis
//
// On the app port we parse each RESP command, prefix the key argument(s) with
// VERSION_TAG, forward to upstream, and (when L2 is configured) maintain the
// replica index in the L2 metadata Redis. On read miss we fall back to a
// peer-fetch through any sibling registered in L2.  The peer-fetch port is a
// transparent byte forwarder so cross-pod fetches never recurse into another
// L2 lookup.
//
// Standard library only — no external Redis client; we reuse the same RESP
// reader/encoder when talking to L2.
package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------- configuration --------------------------------------------------

var (
	listenHost      = envStr("LISTEN_HOST", "0.0.0.0")
	listenPort      = envInt("LISTEN_PORT", 6379)
	peerFetchPort   = envInt("PEER_FETCH_PORT", 6378)
	upstreamHost    = envStr("UPSTREAM_REDIS_HOST", "127.0.0.1")
	upstreamPort    = envInt("UPSTREAM_REDIS_PORT", 6380)
	versionTag      = envStr("VERSION_TAG", "v_default")
	podName         = envStr("POD_NAME", "anon")
	podIP           = envStr("POD_IP", "")
	nodeName        = envStr("NODE_NAME", "unknown")
	l2Host          = envStr("L2_INDEX_HOST", "")
	l2Port          = envInt("L2_INDEX_PORT", 6379)
	peerFetchTOms   = envInt("PEER_FETCH_TIMEOUT_MS", 1500)
	peerFetchTO     = time.Duration(peerFetchTOms) * time.Millisecond
	prefixSeparator = []byte(":")
)

// ---------- command tables -------------------------------------------------

var (
	singleKey   = stringSet("SET", "GET", "GETSET", "SETNX", "SETEX", "PSETEX", "GETEX", "GETDEL",
		"INCR", "INCRBY", "INCRBYFLOAT", "DECR", "DECRBY",
		"APPEND", "STRLEN", "GETRANGE", "SETRANGE",
		"EXPIRE", "EXPIREAT", "PEXPIRE", "PEXPIREAT",
		"TTL", "PTTL", "PERSIST", "TYPE", "DUMP", "RESTORE",
		"HGET", "HSET", "HDEL", "HEXISTS", "HKEYS", "HVALS",
		"HGETALL", "HLEN", "HINCRBY", "HINCRBYFLOAT", "HMGET", "HMSET", "HSCAN", "HSETNX",
		"LPUSH", "RPUSH", "LPOP", "RPOP", "LRANGE", "LLEN",
		"LINDEX", "LSET", "LREM", "LINSERT", "LTRIM", "LPUSHX", "RPUSHX",
		"SADD", "SREM", "SMEMBERS", "SISMEMBER", "SCARD", "SPOP", "SRANDMEMBER", "SSCAN",
		"ZADD", "ZREM", "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE",
		"ZRANK", "ZREVRANK", "ZSCORE", "ZINCRBY", "ZCARD", "ZCOUNT", "ZSCAN",
		"ZLEXCOUNT", "ZRANGEBYLEX", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZPOPMIN", "ZPOPMAX",
		"GETBIT", "SETBIT", "BITCOUNT", "BITPOS")

	objectLike = stringSet("OBJECT", "PFCOUNT")

	multiKeyAll = stringSet("DEL", "EXISTS", "MGET", "UNLINK", "TOUCH")

	msetStyle = stringSet("MSET", "MSETNX")

	pairKey = stringSet("RENAME", "RENAMENX", "COPY", "SMOVE", "LMOVE", "BLMOVE")

	passthrough = stringSet("PING", "ECHO", "INFO", "COMMAND", "CLUSTER", "CLIENT", "CONFIG",
		"DEBUG", "SELECT", "AUTH", "HELLO", "QUIT", "FLUSHDB", "FLUSHALL",
		"DBSIZE", "SCRIPT", "EVAL", "EVALSHA", "WAIT", "MULTI", "EXEC",
		"DISCARD", "WATCH", "UNWATCH", "PUBLISH", "MONITOR",
		"ROLE", "LATENCY", "MEMORY", "TIME", "LASTSAVE", "SAVE", "BGSAVE",
		"BGREWRITEAOF", "SLOWLOG", "REPLICAOF", "SLAVEOF", "SHUTDOWN",
		"KEYS", "SCAN", "RANDOMKEY", "RESET", "SUBSCRIBE", "UNSUBSCRIBE",
		"PSUBSCRIBE", "PUNSUBSCRIBE", "READONLY", "READWRITE",
		"FUNCTION", "ACL")

	writeRegister = stringSet("SET", "GETSET", "SETNX", "SETEX", "PSETEX", "GETEX",
		"INCR", "INCRBY", "INCRBYFLOAT", "DECR", "DECRBY",
		"APPEND", "SETRANGE", "SETBIT",
		"HSET", "HSETNX", "HMSET", "HINCRBY", "HINCRBYFLOAT",
		"LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LSET", "LINSERT",
		"SADD",
		"ZADD", "ZINCRBY",
		"RESTORE", "COPY")

	readMiss = stringSet("GET", "HGET", "HGETALL", "HMGET",
		"LINDEX", "LRANGE",
		"ZSCORE", "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE",
		"DUMP")
)

// ---------- helpers --------------------------------------------------------

func envStr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func stringSet(items ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(items))
	for _, s := range items {
		m[s] = struct{}{}
	}
	return m
}

func has(set map[string]struct{}, s string) bool {
	_, ok := set[s]
	return ok
}

func upstreamAddr() string {
	return fmt.Sprintf("%s:%d", upstreamHost, upstreamPort)
}

func l2Addr() string {
	return fmt.Sprintf("%s:%d", l2Host, l2Port)
}

func versionPrefix(key []byte) []byte {
	out := make([]byte, 0, len(versionTag)+1+len(key))
	out = append(out, versionTag...)
	out = append(out, prefixSeparator...)
	out = append(out, key...)
	return out
}

// ---------- RESP parser / encoder ------------------------------------------
//
// Returned value:
//   - byte slice for bulk strings, simple strings, errors
//   - int64 for integer replies
//   - []interface{} for arrays
//   - nil for null replies (*-1 / $-1)
//
// rawBytes is the on-the-wire representation of the entire value, useful for
// transparent forwarding of unrecognised inputs.

func readResp(r *bufio.Reader) (rawBytes []byte, parsed interface{}, err error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, nil, fmt.Errorf("malformed RESP line: %q", line)
	}
	raw := append([]byte(nil), line...)
	head := line[0]
	body := line[1 : len(line)-2]

	switch head {
	case '*':
		n, err := strconv.Atoi(string(body))
		if err != nil {
			return nil, nil, err
		}
		if n < 0 {
			return raw, nil, nil
		}
		items := make([]interface{}, n)
		for i := 0; i < n; i++ {
			subRaw, sub, err := readResp(r)
			if err != nil {
				return nil, nil, err
			}
			raw = append(raw, subRaw...)
			items[i] = sub
		}
		return raw, items, nil

	case '$':
		n, err := strconv.Atoi(string(body))
		if err != nil {
			return nil, nil, err
		}
		if n < 0 {
			return raw, nil, nil
		}
		data := make([]byte, n)
		if _, err := io.ReadFull(r, data); err != nil {
			return nil, nil, err
		}
		crlf := make([]byte, 2)
		if _, err := io.ReadFull(r, crlf); err != nil {
			return nil, nil, err
		}
		raw = append(raw, data...)
		raw = append(raw, crlf...)
		return raw, data, nil

	case '+', '-':
		return raw, append([]byte(nil), body...), nil

	case ':':
		v, err := strconv.ParseInt(string(body), 10, 64)
		if err != nil {
			return nil, nil, err
		}
		return raw, v, nil

	default:
		// Inline command (not RESP framed). Split by space.
		parts := strings.Fields(string(line[:len(line)-2]))
		items := make([]interface{}, len(parts))
		for i, p := range parts {
			items[i] = []byte(p)
		}
		return raw, items, nil
	}
}

func encodeArray(items [][]byte) []byte {
	total := len(strconv.Itoa(len(items))) + 3
	for _, it := range items {
		total += len(strconv.Itoa(len(it))) + 5 + len(it)
	}
	buf := make([]byte, 0, total)
	buf = append(buf, '*')
	buf = strconv.AppendInt(buf, int64(len(items)), 10)
	buf = append(buf, '\r', '\n')
	for _, it := range items {
		buf = append(buf, '$')
		buf = strconv.AppendInt(buf, int64(len(it)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, it...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}

func encodeBulk(value []byte) []byte {
	if value == nil {
		return []byte("$-1\r\n")
	}
	buf := make([]byte, 0, 16+len(value))
	buf = append(buf, '$')
	buf = strconv.AppendInt(buf, int64(len(value)), 10)
	buf = append(buf, '\r', '\n')
	buf = append(buf, value...)
	buf = append(buf, '\r', '\n')
	return buf
}

func argvFromParsed(parsed interface{}) ([][]byte, bool) {
	arr, ok := parsed.([]interface{})
	if !ok || len(arr) == 0 {
		return nil, false
	}
	argv := make([][]byte, len(arr))
	for i, x := range arr {
		b, ok := x.([]byte)
		if !ok {
			return nil, false
		}
		argv[i] = b
	}
	return argv, true
}

// ---------- command rewriting ---------------------------------------------

func rewriteCommand(argv [][]byte) [][]byte {
	if len(argv) == 0 {
		return nil
	}
	cmd := strings.ToUpper(string(argv[0]))

	if has(passthrough, cmd) {
		return nil
	}

	if has(singleKey, cmd) {
		if len(argv) < 2 {
			return nil
		}
		out := make([][]byte, len(argv))
		out[0] = argv[0]
		out[1] = versionPrefix(argv[1])
		copy(out[2:], argv[2:])
		return out
	}

	if has(objectLike, cmd) {
		if len(argv) < 3 {
			return nil
		}
		out := make([][]byte, len(argv))
		out[0] = argv[0]
		out[1] = argv[1]
		out[2] = versionPrefix(argv[2])
		copy(out[3:], argv[3:])
		return out
	}

	if has(multiKeyAll, cmd) {
		if len(argv) < 2 {
			return nil
		}
		out := make([][]byte, len(argv))
		out[0] = argv[0]
		for i := 1; i < len(argv); i++ {
			out[i] = versionPrefix(argv[i])
		}
		return out
	}

	if has(msetStyle, cmd) {
		out := make([][]byte, len(argv))
		out[0] = argv[0]
		for i := 1; i < len(argv); i++ {
			if (i-1)%2 == 0 {
				out[i] = versionPrefix(argv[i])
			} else {
				out[i] = argv[i]
			}
		}
		return out
	}

	if has(pairKey, cmd) {
		if len(argv) < 3 {
			return nil
		}
		out := make([][]byte, len(argv))
		out[0] = argv[0]
		out[1] = versionPrefix(argv[1])
		out[2] = versionPrefix(argv[2])
		copy(out[3:], argv[3:])
		return out
	}

	return nil
}

// ---------- L2 client ------------------------------------------------------

// L2Client is a minimal Redis client used solely by the proxy's hot path. It
// keeps a single connection guarded by a mutex; reconnects on error. All
// errors are swallowed and counted — L2 is best-effort.
type L2Client struct {
	addr   string
	mu     sync.Mutex
	conn   net.Conn
	r      *bufio.Reader
	errors uint64
}

func newL2Client(addr string) *L2Client {
	return &L2Client{addr: addr}
}

func (l *L2Client) ensureConn() error {
	if l.conn != nil {
		return nil
	}
	c, err := net.DialTimeout("tcp", l.addr, 2*time.Second)
	if err != nil {
		return err
	}
	l.conn = c
	l.r = bufio.NewReaderSize(c, 64*1024)
	return nil
}

func (l *L2Client) reset() {
	if l.conn != nil {
		_ = l.conn.Close()
	}
	l.conn = nil
	l.r = nil
}

func (l *L2Client) call(args ...[]byte) (interface{}, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for retry := 0; retry < 2; retry++ {
		if err := l.ensureConn(); err != nil {
			l.errors++
			return nil, err
		}
		_ = l.conn.SetDeadline(time.Now().Add(2 * time.Second))
		if _, err := l.conn.Write(encodeArray(args)); err != nil {
			l.reset()
			continue
		}
		_, parsed, err := readResp(l.r)
		if err != nil {
			l.reset()
			continue
		}
		_ = l.conn.SetDeadline(time.Time{})
		return parsed, nil
	}
	l.errors++
	return nil, errors.New("L2 call failed after retry")
}

func (l *L2Client) hSet(key, field, value []byte) error {
	_, err := l.call([]byte("HSET"), key, field, value)
	return err
}

func (l *L2Client) hGet(key, field []byte) ([]byte, error) {
	res, err := l.call([]byte("HGET"), key, field)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	if b, ok := res.([]byte); ok {
		return b, nil
	}
	return nil, fmt.Errorf("unexpected reply type for HGET: %T", res)
}

func (l *L2Client) hGetAll(key []byte) (map[string][]byte, error) {
	res, err := l.call([]byte("HGETALL"), key)
	if err != nil {
		return nil, err
	}
	arr, ok := res.([]interface{})
	if !ok {
		return nil, nil
	}
	out := make(map[string][]byte, len(arr)/2)
	for i := 0; i+1 < len(arr); i += 2 {
		k, _ := arr[i].([]byte)
		v, _ := arr[i+1].([]byte)
		if k != nil {
			out[string(k)] = v
		}
	}
	return out, nil
}

func (l *L2Client) selfRegister() error {
	ep := ""
	if podIP != "" {
		ep = fmt.Sprintf("%s:%d", podIP, peerFetchPort)
	}
	if err := l.hSet([]byte("nodemap"), []byte(podName), []byte(ep)); err != nil {
		return err
	}
	if err := l.hSet([]byte("node_zone"), []byte(podName), []byte(nodeName)); err != nil {
		return err
	}
	if err := l.hSet([]byte("versionmap"), []byte(podName), []byte(versionTag)); err != nil {
		return err
	}
	return nil
}

// registerWrite is called as a goroutine after every successful write command.
// Best effort; logs on errors and otherwise does nothing.
func (l *L2Client) registerWrite(cacheKey []byte) {
	if l == nil {
		return
	}
	key := append([]byte("replicas:"+versionTag+":"), cacheKey...)
	field := []byte(podName)
	value := []byte(strconv.FormatInt(time.Now().Unix(), 10))
	if err := l.hSet(key, field, value); err != nil {
		log.Printf("L2 register_write failed: %v", err)
	}
}

// ---------- peer fetch -----------------------------------------------------

// peerFetchValue tries to pull the bulk value of prefixedKey from a sibling
// holding cacheKey in L2's Sk. Returns nil on miss / failure.
func peerFetchValue(l2 *L2Client, cacheKey, prefixedKey []byte, originalCmd []byte) []byte {
	if l2 == nil {
		return nil
	}
	skKey := append([]byte("replicas:"+versionTag+":"), cacheKey...)
	sk, err := l2.hGetAll(skKey)
	if err != nil {
		return nil
	}
	var peers []string
	for p := range sk {
		if p != podName {
			peers = append(peers, p)
		}
	}
	if len(peers) == 0 {
		return nil
	}

	// Try up to 3 peers.
	limit := 3
	if len(peers) < limit {
		limit = len(peers)
	}
	for i := 0; i < limit; i++ {
		ep, err := l2.hGet([]byte("nodemap"), []byte(peers[i]))
		if err != nil || ep == nil {
			continue
		}
		addr := string(ep)
		if !strings.Contains(addr, ":") {
			continue
		}
		c, err := net.DialTimeout("tcp", addr, peerFetchTO)
		if err != nil {
			continue
		}
		_ = c.SetDeadline(time.Now().Add(peerFetchTO))
		if _, err := c.Write(encodeArray([][]byte{originalCmd, prefixedKey})); err != nil {
			_ = c.Close()
			continue
		}
		r := bufio.NewReaderSize(c, 64*1024)
		_, parsed, err := readResp(r)
		_ = c.Close()
		if err != nil {
			continue
		}
		if parsed == nil {
			continue
		}
		if b, ok := parsed.([]byte); ok && b != nil {
			return b
		}
	}
	return nil
}

// ---------- app-side connection handler -----------------------------------

func handleAppConn(client net.Conn, l2 *L2Client) {
	defer func() { _ = client.Close() }()

	upstream, err := net.Dial("tcp", upstreamAddr())
	if err != nil {
		log.Printf("upstream connect failed: %v", err)
		return
	}
	defer func() { _ = upstream.Close() }()

	// TCP_NODELAY on both sides: small RESP commands shouldn't sit waiting for
	// Nagle-coalescing under serial request/response.
	if tc, ok := client.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	if tu, ok := upstream.(*net.TCPConn); ok {
		_ = tu.SetNoDelay(true)
	}

	cr := bufio.NewReaderSize(client, 64*1024)
	ur := bufio.NewReaderSize(upstream, 64*1024)
	cw := bufio.NewWriterSize(client, 64*1024)
	uw := bufio.NewWriterSize(upstream, 64*1024)

	for {
		reqRaw, parsed, err := readResp(cr)
		if err != nil {
			return
		}

		argv, ok := argvFromParsed(parsed)

		if ok && len(argv) > 0 {
			cmdName := strings.ToUpper(string(argv[0]))
			newArgv := rewriteCommand(argv)
			if newArgv == nil {
				if _, err := uw.Write(reqRaw); err != nil {
					return
				}
			} else {
				if _, err := uw.Write(encodeArray(newArgv)); err != nil {
					return
				}
			}
			if err := uw.Flush(); err != nil {
				return
			}

			replyRaw, _, err := readResp(ur)
			if err != nil {
				return
			}

			// Async L2 register on successful writes (non-error reply).
			if l2 != nil && has(writeRegister, cmdName) && len(argv) >= 2 && len(replyRaw) > 0 && replyRaw[0] != '-' {
				cacheKey := append([]byte(nil), argv[1]...)
				go l2.registerWrite(cacheKey)
			}

			// Read miss path: peer fetch on $-1\r\n.
			var replaced []byte
			if l2 != nil && has(readMiss, cmdName) && len(argv) >= 2 && bytes.Equal(replyRaw, []byte("$-1\r\n")) {
				cacheKey := argv[1]
				prefixed := versionPrefix(cacheKey)
				value := peerFetchValue(l2, cacheKey, prefixed, argv[0])
				if value != nil {
					if _, err := uw.Write(encodeArray([][]byte{[]byte("SET"), prefixed, value})); err != nil {
						return
					}
					if err := uw.Flush(); err != nil {
						return
					}
					if _, _, err := readResp(ur); err != nil {
						return
					}
					go l2.registerWrite(append([]byte(nil), cacheKey...))
					replaced = encodeBulk(value)
				}
			}

			if replaced != nil {
				if _, err := cw.Write(replaced); err != nil {
					return
				}
			} else {
				if _, err := cw.Write(replyRaw); err != nil {
					return
				}
			}
			if err := cw.Flush(); err != nil {
				return
			}
		} else {
			// Inline / non-multi-bulk → forward raw.
			if _, err := uw.Write(reqRaw); err != nil {
				return
			}
			if err := uw.Flush(); err != nil {
				return
			}
			replyRaw, _, err := readResp(ur)
			if err != nil {
				return
			}
			if _, err := cw.Write(replyRaw); err != nil {
				return
			}
			if err := cw.Flush(); err != nil {
				return
			}
		}
	}
}

// ---------- peer-fetch handler ---------------------------------------------

func handlePeerConn(client net.Conn) {
	defer func() { _ = client.Close() }()

	upstream, err := net.Dial("tcp", upstreamAddr())
	if err != nil {
		log.Printf("peer-fetch upstream connect failed: %v", err)
		return
	}
	defer func() { _ = upstream.Close() }()

	if tc, ok := client.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	if tu, ok := upstream.(*net.TCPConn); ok {
		_ = tu.SetNoDelay(true)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(upstream, client)
		if tu, ok := upstream.(*net.TCPConn); ok {
			_ = tu.CloseWrite()
		}
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(client, upstream)
		if tc, ok := client.(*net.TCPConn); ok {
			_ = tc.CloseWrite()
		}
	}()
	wg.Wait()
}

// ---------- main -----------------------------------------------------------

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.SetPrefix("[sentrycache.proxy] ")

	var l2 *L2Client
	if l2Host != "" {
		l2 = newL2Client(l2Addr())
		if _, err := l2.call([]byte("PING")); err != nil {
			log.Printf("L2 ping failed at %s: %v; running degraded", l2Addr(), err)
			l2 = nil
		} else {
			if err := l2.selfRegister(); err != nil {
				log.Printf("L2 self-register failed: %v", err)
			} else {
				log.Printf("L2 self-register pod=%s ip=%s node=%s ver=%s",
					podName, podIP, nodeName, versionTag)
			}
		}
	} else {
		log.Printf("L2_INDEX_HOST not set; running standalone (no L2 / no peer-fetch)")
	}

	appAddr := fmt.Sprintf("%s:%d", listenHost, listenPort)
	peerAddr := fmt.Sprintf("%s:%d", listenHost, peerFetchPort)

	appLn, err := net.Listen("tcp", appAddr)
	if err != nil {
		log.Fatalf("app listen %s: %v", appAddr, err)
	}
	peerLn, err := net.Listen("tcp", peerAddr)
	if err != nil {
		log.Fatalf("peer listen %s: %v", peerAddr, err)
	}

	log.Printf("proxy up app=%s peer=%s upstream=%s ver=%s pod=%s/%s/%s l2=%s",
		appAddr, peerAddr, upstreamAddr(), versionTag, podName, podIP, nodeName,
		envStr("L2_INDEX_HOST", "off"))

	go func() {
		for {
			c, err := peerLn.Accept()
			if err != nil {
				log.Printf("peer accept: %v", err)
				continue
			}
			go handlePeerConn(c)
		}
	}()

	for {
		c, err := appLn.Accept()
		if err != nil {
			log.Printf("app accept: %v", err)
			continue
		}
		go handleAppConn(c, l2)
	}
}
