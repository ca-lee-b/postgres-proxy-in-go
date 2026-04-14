package pool

import (
	"net"
	"testing"

	"github.com/ca-lee-b/postgres-proxy-go/internal/config"
)

func Test_Lifecycle(t *testing.T) {
	config := config.BackendConfig{}
	p := New(config, 5, "test")

	// Inject fake connections
	_, c1, _ := localSocketPair()
	_, c2, _ := localSocketPair()

	bc1 := &BackendConn{conn: c1}
	bc2 := &BackendConn{conn: c2}

	p.conns = append(p.conns, bc1, bc2)

	total, inUse := p.Stats()
	t.Logf("initial state  : total=%d in_use=%d", total, inUse)

	bc1.inUse = false
	out, err := p.Checkout()
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}
	total, inUse = p.Stats()
	t.Logf("after checkout : total=%d in_use=%d (got conn %p)", total, inUse, out)

	out2, _ := p.Checkout()
	total, inUse = p.Stats()
	t.Logf("after second checkout : total=%d in_use=%d (got conn %p)", total, inUse, out)
	t.Logf("                      : got conn %p (same? %v)", out2, out == out2) // should be different

	p.Return(out)
	p.Return(out2)
	total, inUse = p.Stats()
	t.Logf("after returns  : total=%d in_use=%d", total, inUse)

	out2, _ = p.Checkout()
	t.Logf("second checkout: got conn %p (same? %v)", out2, out == out2) // should be same
}

// localSocketPair creates a local connected socket pair without needing Postgres
func localSocketPair() (server, client net.Conn, err error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}

	defer ln.Close()
	ch := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		ch <- c
	}()

	client, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		return nil, nil, err
	}

	server = <-ch
	return server, client, nil
}
