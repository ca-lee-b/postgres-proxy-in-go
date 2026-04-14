// Package pool manages persistent connections to a single postgres db
// Transaction-mode pooling: a backend connection is checked out
// for the duration of a transaction and then returned
package pool

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/ca-lee-b/postgres-proxy-go/internal/config"
	"github.com/ca-lee-b/postgres-proxy-go/internal/protocol"
)

// BackendConn is a live TCP connection to a Postgres database post-startup/auth
type BackendConn struct {
	conn     net.Conn
	inUse    bool
	lastUsed time.Time
}

func (b *BackendConn) Read(p []byte) (int, error)  { return b.conn.Read(p) }
func (b *BackendConn) Write(p []byte) (int, error) { return b.conn.Write(p) }
func (b *BackendConn) Close() error                { return b.conn.Close() }

type Pool struct {
	cfg     config.BackendConfig
	mu      sync.Mutex
	conns   []*BackendConn
	maxSize int
	label   string // e.g. "primary" or "replica-2"
}

// New creates a pool but does not connect (it lazy connects)
func New(cfg config.BackendConfig, maxSize int, label string) *Pool {
	return &Pool{
		cfg:     cfg,
		conns:   make([]*BackendConn, 0, maxSize),
		maxSize: maxSize,
		label:   label,
	}
}

// Checkout returns an idle connection or creates one if under the limit
// Returns an error if a pool is exhausted
func (p *Pool) Checkout() (*BackendConn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, bc := range p.conns {
		if !bc.inUse {
			bc.inUse = true
			bc.lastUsed = time.Now()
			return bc, nil
		}
	}

	if len(p.conns) < p.maxSize {
		bc, err := p.dial()
		if err != nil {
			return nil, fmt.Errorf("[pool:%s] dial failed: %w", p.label, err)
		}

		bc.inUse = true
		p.conns = append(p.conns, bc)
		return bc, nil
	}

	return nil, fmt.Errorf("[pool:%s] exhausted (%d/%d connections in use)", p.label, len(p.conns), p.maxSize)
}

func (p *Pool) Return(bc *BackendConn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	bc.inUse = false
	bc.lastUsed = time.Now()
}

func (p *Pool) Discard(bc *BackendConn) {
	_ = bc.conn.Close()
	p.mu.Lock()
	defer p.mu.Unlock()

	for i, c := range p.conns {
		if c == bc {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			return
		}
	}
}

// Stats returns (total, inUse)
func (p *Pool) Stats() (int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	inUse := 0

	for _, c := range p.conns {
		if c.inUse {
			inUse++
		}
	}

	return len(p.conns), inUse
}

// dial opens a raw TCP connection to postgres and initiates the handshake
func (p *Pool) dial() (*BackendConn, error) {
	addr := fmt.Sprintf("%s:%d", p.cfg.Host, p.cfg.Port)
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}

	if err := doStartup(conn, p.cfg); err != nil {
		conn.Close()
		return nil, err
	}

	return &BackendConn{
		conn: conn,
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
			case 5: // MD5Password not implemented
			default:
				return fmt.Errorf("unsupported auth type: %d", authType)
			}

		case protocol.MsgErrorResponse:
			return fmt.Errorf("backend error during startup: %s", parseErrorMessage(msg.Payload))

		case protocol.MsgReadyForQuery:
			return nil

		// ignore for now
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
