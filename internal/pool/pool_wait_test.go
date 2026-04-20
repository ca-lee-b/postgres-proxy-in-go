package pool

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ca-lee-b/postgres-proxy-go/internal/config"
)

func Test_Wait(t *testing.T) {
	cfg := config.BackendConfig{}
	p := New(cfg, "test", Options{
		MaxSize: 1,
		DialFn: func() (net.Conn, error) {
			s, _, err := localSocketPair()
			return s, err
		},
	})
	ctx := context.Background()

	bc1, err := p.Checkout(ctx)
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}

	go func() {
		time.Sleep(30 * time.Millisecond)
		p.Return(bc1)
	}()

	start := time.Now()
	bc2, err := p.Checkout(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("second checkout: %v", err)
	}
	if elapsed < 20*time.Millisecond {
		t.Fatalf("expected to wait for release, waited only %v", elapsed)
	}
	p.Return(bc2)
	_ = p.Close()
}

func Test_CtxCancel(t *testing.T) {
	cfg := config.BackendConfig{}
	p := New(cfg, "test", Options{
		MaxSize: 1,
		DialFn: func() (net.Conn, error) {
			s, _, err := localSocketPair()
			return s, err
		},
	})
	ctx := context.Background()

	bc1, err := p.Checkout(ctx)
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}

	ctx2, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	_, err = p.Checkout(ctx2)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	p.Return(bc1)
	_ = p.Close()
}

func Test_AcquireTimeout(t *testing.T) {
	cfg := config.BackendConfig{}
	p := New(cfg, "test", Options{
		MaxSize:        1,
		AcquireTimeout: 50 * time.Millisecond,
		DialFn: func() (net.Conn, error) {
			s, _, err := localSocketPair()
			return s, err
		},
	})
	ctx := context.Background()

	bc1, err := p.Checkout(ctx)
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}

	_, err = p.Checkout(ctx)
	if !errors.Is(err, ErrPoolTimeout) {
		t.Fatalf("want ErrPoolTimeout, got %v", err)
	}
	p.Return(bc1)
	_ = p.Close()
}

func Test_Stale(t *testing.T) {
	cfg := config.BackendConfig{}
	var dials atomic.Int32
	p := New(cfg, "test", Options{
		MaxSize: 5,
		MaxLife: time.Nanosecond,
		DialFn: func() (net.Conn, error) {
			dials.Add(1)
			s, _, err := localSocketPair()
			return s, err
		},
	})
	ctx := context.Background()

	bc1, err := p.Checkout(ctx)
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}
	p.Return(bc1)
	time.Sleep(5 * time.Millisecond)

	bc2, err := p.Checkout(ctx)
	if err != nil {
		t.Fatalf("second checkout: %v", err)
	}
	p.Return(bc2)

	if dials.Load() < 2 {
		t.Fatalf("expected a recycle dial after max life, dials=%d", dials.Load())
	}
	_ = p.Close()
}

func Test_Reaper(t *testing.T) {
	cfg := config.BackendConfig{}
	p := New(cfg, "test", Options{
		MaxSize:           5,
		MinSize:           1,
		MaxIdle:           10 * time.Millisecond,
		IdleCheckInterval: 20 * time.Millisecond,
		DialFn: func() (net.Conn, error) {
			s, _, err := localSocketPair()
			return s, err
		},
	})
	p.Start()

	ctx := context.Background()
	var held []*BackendConn
	for i := 0; i < 3; i++ {
		bc, err := p.Checkout(ctx)
		if err != nil {
			t.Fatalf("checkout %d: %v", i, err)
		}
		held = append(held, bc)
	}
	for _, bc := range held {
		p.Return(bc)
	}

	total, _, _, _ := p.Stats()
	if total != 3 {
		t.Fatalf("want 3 total after seeding, got %d", total)
	}

	time.Sleep(120 * time.Millisecond)

	total, _, _, _ = p.Stats()
	t.Logf("after reaper: total=%d", total)
	if total != 1 {
		t.Fatalf("want total 1 (minSize), got %d", total)
	}
	_ = p.Close()
}

func Test_Close(t *testing.T) {
	cfg := config.BackendConfig{}
	p := New(cfg, "test", Options{
		MaxSize: 1,
		DialFn: func() (net.Conn, error) {
			s, _, err := localSocketPair()
			return s, err
		},
	})
	ctx := context.Background()

	bc1, err := p.Checkout(ctx)
	if err != nil {
		t.Fatalf("checkout: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := p.Checkout(ctx)
		errCh <- err
	}()

	time.Sleep(50 * time.Millisecond)
	_ = p.Close()

	select {
	case err := <-errCh:
		if !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("want ErrPoolClosed, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not unblock")
	}

	p.Return(bc1)
}

func Test_NoLeak(t *testing.T) {
	cfg := config.BackendConfig{}
	p := New(cfg, "test", Options{
		MaxSize: 5,
		DialFn: func() (net.Conn, error) {
			s, _, err := localSocketPair()
			return s, err
		},
	})
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bc, err := p.Checkout(ctx)
			if err != nil {
				return
			}
			time.Sleep(time.Millisecond)
			p.Return(bc)
		}()
	}
	wg.Wait()

	total, inUse, idle, _ := p.Stats()
	t.Logf("after burst: total=%d in_use=%d idle=%d", total, inUse, idle)
	if total > 5 {
		t.Fatalf("want at most 5 conns, got %d", total)
	}
	_ = p.Close()
}
