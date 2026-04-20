package pool

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/ca-lee-b/postgres-proxy-go/internal/config"
)

func Test_Lifecycle(t *testing.T) {
	cfg := config.BackendConfig{}
	var dials atomic.Int32
	p := New(cfg, "test", Options{
		MaxSize: 5,
		DialFn: func() (net.Conn, error) {
			dials.Add(1)
			s, _, err := localSocketPair()
			return s, err
		},
	})

	ctx := context.Background()

	total, inUse, idle, waiting := p.Stats()
	t.Logf("initial state  : total=%d in_use=%d idle=%d waiting=%d dials=%d", total, inUse, idle, waiting, dials.Load())

	out, err := p.Checkout(ctx)
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}
	total, inUse, idle, waiting = p.Stats()
	t.Logf("after checkout : total=%d in_use=%d idle=%d waiting=%d (got conn %p)", total, inUse, idle, waiting, out)

	out2, err := p.Checkout(ctx)
	if err != nil {
		t.Fatalf("second checkout: %v", err)
	}
	total, inUse, idle, waiting = p.Stats()
	t.Logf("after second checkout : total=%d in_use=%d idle=%d waiting=%d", total, inUse, idle, waiting)
	t.Logf("                      : got conn %p (same? %v)", out2, out == out2)

	p.Return(out)
	p.Return(out2)
	total, inUse, idle, waiting = p.Stats()
	t.Logf("after returns  : total=%d in_use=%d idle=%d waiting=%d", total, inUse, idle, waiting)

	out2, err = p.Checkout(ctx)
	if err != nil {
		t.Fatalf("third checkout: %v", err)
	}
	t.Logf("third checkout: got conn %p (same as first? %v)", out2, out == out2)
	p.Return(out2)

	_ = p.Close()
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
