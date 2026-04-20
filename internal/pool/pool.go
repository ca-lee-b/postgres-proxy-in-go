// Package pool manages persistent connections to a single postgres db
// Transaction-mode pooling: a backend connection is checked out
// for the duration of a transaction and then returned
package pool

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ca-lee-b/postgres-proxy-go/internal/config"
	"github.com/ca-lee-b/postgres-proxy-go/internal/protocol"
)

// Sentinel errors for Checkout.
var (
	ErrPoolTimeout = errors.New("[pool] acquire timeout")
	ErrPoolClosed  = errors.New("[pool] closed")
)

// BackendConn is a live TCP connection to a Postgres database post-startup/auth
type BackendConn struct {
	conn      net.Conn
	createdAt time.Time
	lastUsed  time.Time
}

func (b *BackendConn) Read(p []byte) (int, error)  { return b.conn.Read(p) }
func (b *BackendConn) Write(p []byte) (int, error) { return b.conn.Write(p) }
func (b *BackendConn) Close() error                { return b.conn.Close() }

// Options configures pool behaviour. Zero values use sensible defaults from New.
type Options struct {
	MaxSize           int
	MinSize           int
	MaxIdle           time.Duration
	MaxLife           time.Duration
	AcquireTimeout    time.Duration
	IdleCheckInterval time.Duration
	// DialFn, if set, replaces TCP dial + Postgres startup (used in tests).
	DialFn func() (net.Conn, error)
}

type Pool struct {
	cfg     config.BackendConfig
	label   string
	maxSize int
	minSize int

	maxIdle           time.Duration
	maxLife           time.Duration
	acquireTimeout    time.Duration
	idleCheckInterval time.Duration

	idle chan *BackendConn // idle connections (capacity maxSize)
	sem  chan struct{}     // one token per live connection (capacity maxSize)
	done chan struct{}     // closed when pool is shutting down

	dialFn func() (net.Conn, error)

	mu     sync.Mutex
	closed bool

	startOnce sync.Once
	closeOnce sync.Once

	waitCount atomic.Int64
	waitNanos atomic.Int64
	timeouts  atomic.Int64
	waiting   atomic.Int64
}

// New creates a pool. Call Start() to enable the reaper and async warmup.
func New(cfg config.BackendConfig, label string, opts Options) *Pool {
	maxSize := opts.MaxSize
	if maxSize <= 0 {
		maxSize = 10
	}
	minSize := opts.MinSize
	if minSize < 0 {
		minSize = 0
	}
	if minSize > maxSize {
		minSize = maxSize
	}

	p := &Pool{
		cfg:               cfg,
		label:             label,
		maxSize:           maxSize,
		minSize:           minSize,
		maxIdle:           opts.MaxIdle,
		maxLife:           opts.MaxLife,
		acquireTimeout:    opts.AcquireTimeout,
		idleCheckInterval: opts.IdleCheckInterval,
		idle:              make(chan *BackendConn, maxSize),
		sem:               make(chan struct{}, maxSize),
		done:              make(chan struct{}),
		dialFn:            opts.DialFn,
	}
	return p
}

// Start launches the reaper (if idleCheckInterval > 0) and async warmup for minSize.
// Safe to call once; subsequent calls are no-ops.
func (p *Pool) Start() {
	p.startOnce.Do(func() {
		if p.idleCheckInterval > 0 {
			go p.reaperLoop()
		}
		go p.warmup()
	})
}

func (p *Pool) warmup() {
	if p.minSize <= 0 {
		return
	}
	for i := 0; i < p.minSize; i++ {
		p.mu.Lock()
		closed := p.closed
		p.mu.Unlock()
		if closed {
			return
		}

		select {
		case p.sem <- struct{}{}:
			bc, err := p.dial()
			if err != nil {
				<-p.sem
				log.Printf("[pool:%s] warmup dial: %v", p.label, err)
				time.Sleep(50 * time.Millisecond)
				continue
			}
			bc.createdAt = time.Now()
			bc.lastUsed = time.Now()
			select {
			case p.idle <- bc:
			default:
				p.destroy(bc)
			}
		default:
			return
		}
	}
}

func (p *Pool) reaperLoop() {
	t := time.NewTicker(p.idleCheckInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			p.reapIdle()
		case <-p.done:
			return
		}
	}
}

func (p *Pool) reapIdle() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	var drained []*BackendConn
	for {
		select {
		case bc := <-p.idle:
			drained = append(drained, bc)
		default:
			goto process
		}
	}

process:
	for _, bc := range drained {
		total := len(p.sem)
		switch {
		case p.isMaxLifeStale(bc):
			p.destroy(bc)
		case p.isMaxIdleStale(bc) && total > p.minSize:
			p.destroy(bc)
		default:
			select {
			case p.idle <- bc:
			default:
				p.destroy(bc)
			}
		}
	}
}

func (p *Pool) isMaxLifeStale(bc *BackendConn) bool {
	if p.maxLife <= 0 || bc == nil {
		return false
	}
	return time.Since(bc.createdAt) > p.maxLife
}

func (p *Pool) isMaxIdleStale(bc *BackendConn) bool {
	if p.maxIdle <= 0 || bc == nil {
		return false
	}
	return time.Since(bc.lastUsed) > p.maxIdle
}

func (p *Pool) isStale(bc *BackendConn) bool {
	if bc == nil {
		return true
	}
	return p.isMaxLifeStale(bc)
}

// Checkout returns an idle connection, waits for one, or opens a new connection up to maxSize.
func (p *Pool) Checkout(ctx context.Context) (*BackendConn, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	p.mu.Unlock()

	for {
		// 1. Fast path: grab an idle connection.
		select {
		case bc := <-p.idle:
			if p.isStale(bc) {
				p.destroy(bc)
				continue
			}
			bc.lastUsed = time.Now()
			return bc, nil
		default:
		}

		// 2. Try to grow the pool (non-blocking claim on sem).
		select {
		case p.sem <- struct{}{}:
			bc, err := p.dial()
			if err != nil {
				<-p.sem
				return nil, fmt.Errorf("[pool:%s] dial failed: %w", p.label, err)
			}
			now := time.Now()
			bc.createdAt = now
			bc.lastUsed = now
			return bc, nil
		default:
		}

		// 3. Wait for a free connection, cancellation, timeout, or close.
		p.waiting.Add(1)
		p.waitCount.Add(1)
		start := time.Now()

		var timer *time.Timer
		var timerCh <-chan time.Time
		if p.acquireTimeout > 0 {
			timer = time.NewTimer(p.acquireTimeout)
			timerCh = timer.C
		}

		var err error
		var bc *BackendConn
		select {
		case bc = <-p.idle:
			if p.isStale(bc) {
				p.destroy(bc)
				err = nil
				bc = nil
			} else {
				bc.lastUsed = time.Now()
			}
		case <-ctx.Done():
			err = ctx.Err()
			p.timeouts.Add(1)
		case <-timerCh:
			err = ErrPoolTimeout
			p.timeouts.Add(1)
		case <-p.done:
			err = ErrPoolClosed
		}
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}

		p.waitNanos.Add(time.Since(start).Nanoseconds())
		p.waiting.Add(-1)

		if err != nil {
			return nil, err
		}
		if bc != nil {
			return bc, nil
		}
		// Stale conn destroyed; retry.
		continue
	}
}

// Return puts a healthy connection back in the pool.
func (p *Pool) Return(bc *BackendConn) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if bc == nil {
		return
	}
	if closed || p.isStale(bc) {
		p.destroy(bc)
		return
	}
	bc.lastUsed = time.Now()
	select {
	case p.idle <- bc:
	default:
		p.destroy(bc)
	}
}

// Discard closes a broken connection and releases its slot.
func (p *Pool) Discard(bc *BackendConn) {
	if bc == nil {
		return
	}
	_ = bc.conn.Close()
	bc.conn = nil
	p.destroy(bc)
}

func (p *Pool) destroy(bc *BackendConn) {
	if bc == nil {
		return
	}
	if bc.conn != nil {
		_ = bc.conn.Close()
		bc.conn = nil
	}
	select {
	case <-p.sem:
	default:
	}
}

// Stats returns total connections, in-use count, idle count, and current waiters.
func (p *Pool) Stats() (total, inUse, idle, waiting int) {
	total = len(p.sem)
	idle = len(p.idle)
	inUse = total - idle
	if inUse < 0 {
		inUse = 0
	}
	waiting = int(p.waiting.Load())
	return total, inUse, idle, waiting
}

// WaitStats returns aggregate wait metrics for /metrics.
func (p *Pool) WaitStats() (waitCount, waitNanos, timeouts int64) {
	return p.waitCount.Load(), p.waitNanos.Load(), p.timeouts.Load()
}

// Close shuts down the pool: idle connections are closed; in-flight checkouts unblock with ErrPoolClosed.
func (p *Pool) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		close(p.done)

		for {
			select {
			case bc := <-p.idle:
				p.destroy(bc)
			default:
				return
			}
		}
	})
	return nil
}

func (p *Pool) dial() (*BackendConn, error) {
	if p.dialFn != nil {
		c, err := p.dialFn()
		if err != nil {
			return nil, err
		}
		now := time.Now()
		return &BackendConn{conn: c, createdAt: now, lastUsed: now}, nil
	}

	addr := net.JoinHostPort(p.cfg.Host, fmt.Sprintf("%d", p.cfg.Port))
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}

	if err := doStartup(conn, p.cfg); err != nil {
		conn.Close()
		return nil, err
	}

	now := time.Now()
	return &BackendConn{
		conn:      conn,
		createdAt: now,
		lastUsed:  now,
	}, nil
}

// doStartup sends the postgres StartupMessage and handles auth
func doStartup(conn net.Conn, cfg config.BackendConfig) error {
	// Build startup message: length(4) + version(4) + key=value pairs \0 terminated
	params := buildStartupParams(cfg.User, cfg.Database)
	length := 4 + 4 + len(params)

	buf := make([]byte, length)
	buf[0] = byte(length >> 24)
	buf[1] = byte(length >> 16)
	buf[2] = byte(length >> 8)
	buf[3] = byte(length)
	// Protocol version 3.0 = 0x00030000
	buf[4] = 0
	buf[5] = 3
	buf[6] = 0
	buf[7] = 0
	copy(buf[8:], params)

	if _, err := conn.Write(buf); err != nil {
		return err
	}

	// Read messages until ReadyForQuery
	for {
		msg, err := protocol.ReadMessage(conn)
		if err != nil {
			return fmt.Errorf("startup read: %w", err)
		}

		switch msg.Type {
		case protocol.MsgAuthRequest:
			authType := int32(msg.Payload[0])<<24 | int32(msg.Payload[1])<<16 |
				int32(msg.Payload[2])<<8 | int32(msg.Payload[3])
			switch authType {
			case 0: // AuthenticationOk
				// continue reading
			case 3: // CleartextPassword
				if err := sendPassword(conn, cfg.Password); err != nil {
					return err
				}
			case 5: // MD5Password
				if len(msg.Payload) < 8 {
					return fmt.Errorf("md5 auth: short payload (%d bytes)", len(msg.Payload))
				}
				salt := msg.Payload[4:8]
				if err := sendMD5Password(conn, cfg.User, cfg.Password, salt); err != nil {
					return err
				}
			case 10: // SASL (SCRAM-SHA-256)
				if err := doSCRAM(conn, cfg, msg.Payload[4:]); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unsupported auth type: %d", authType)
			}

		case protocol.MsgErrorResponse:
			return fmt.Errorf("backend error during startup: %s", parseErrorMessage(msg.Payload))

		case protocol.MsgReadyForQuery:
			return nil

		case protocol.MsgParameterStatus, protocol.MsgBackendKeyData:
			// skip

		default:
			// ignore unknown messages during startup
		}
	}
}

func buildStartupParams(user, database string) []byte {
	var b []byte
	appendParam := func(k, v string) {
		b = append(b, []byte(k)...)
		b = append(b, 0)
		b = append(b, []byte(v)...)
		b = append(b, 0)
	}
	appendParam("user", user)
	appendParam("database", database)
	appendParam("application_name", "proxy")
	b = append(b, 0) // terminator
	return b
}

func sendPassword(conn net.Conn, password string) error {
	payload := append([]byte(password), 0)
	msg := &protocol.Message{
		Type:    protocol.MsgPassword,
		Payload: payload,
	}

	return protocol.WriteMessage(conn, msg)
}

func parseErrorMessage(payload []byte) string {
	result := ""
	i := 0

	for i < len(payload) {
		tag := payload[i]
		i++
		if tag == 0 {
			break
		}
		start := i
		for i < len(payload) && payload[i] != 0 {
			i++
		}
		val := string(payload[start:i])
		i++ // skip null
		if tag == 'M' {
			result = val
		}
	}
	return result
}
