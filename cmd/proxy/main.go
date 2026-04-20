package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ca-lee-b/postgres-proxy-go/internal/config"
	"github.com/ca-lee-b/postgres-proxy-go/internal/health"
	"github.com/ca-lee-b/postgres-proxy-go/internal/metrics"
	"github.com/ca-lee-b/postgres-proxy-go/internal/pool"
	"github.com/ca-lee-b/postgres-proxy-go/internal/protocol"
	"github.com/ca-lee-b/postgres-proxy-go/internal/router"
)

func main() {
	cfg := loadConfig()

	log.Printf("proxy starting on %s", cfg.ListenAddr)
	log.Printf("primary: %s:%d", cfg.Primary.Host, cfg.Primary.Port)
	for i, r := range cfg.Replicas {
		log.Printf("replica[%d]: %s:%d", i, r.Host, r.Port)
	}

	checker := health.New(time.Duration(cfg.HealthInterval) * time.Second)
	checker.Start(cfg.Primary, cfg.Replicas)

	rt := router.NewWithChecker(cfg, checker)

	metricsAddr := cfg.MetricsAddr
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}
	metrics.StartServer(metricsAddr, func() metrics.PoolSnapshot {
		var stats []metrics.PoolStat
		var waitTotal, waitNanos, timeouts int64
		for i, p := range rt.AllPools() {
			label := "primary"
			if i > 0 {
				label = fmt.Sprintf("replica-%d", i-1)
			}
			total, inUse, idle, waiting := p.Stats()
			stats = append(stats, metrics.PoolStat{
				Label: label, Total: total, InUse: inUse, Idle: idle, Waiting: waiting,
			})
			wc, wn, to := p.WaitStats()
			waitTotal += wc
			waitNanos += wn
			timeouts += to
		}
		return metrics.PoolSnapshot{
			Pools:               stats,
			PoolWaitTotal:       waitTotal,
			PoolWaitSeconds:     float64(waitNanos) / 1e9,
			PoolAcquireTimeouts: timeouts,
		}
	})

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("listening on %s  (metrics on %s)", cfg.ListenAddr, metricsAddr)

	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		log.Println("shutting down...")
		for _, p := range rt.AllPools() {
			_ = p.Close()
		}
		ln.Close()
		os.Exit(0)
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		metrics.Global.ClientsTotal.Inc()
		metrics.Global.ClientsActive.Inc()
		go func() {
			defer metrics.Global.ClientsActive.Add(-1)
			handleClient(conn, rt, cfg)
		}()
	}
}

func handleClient(clientConn net.Conn, rt *router.Router, cfg *config.Config) {
	defer clientConn.Close()
	remote := clientConn.RemoteAddr().String()
	log.Printf("[client %s] connected", remote)
	defer log.Printf("[client %s] disconnected", remote)

	startup, err := protocol.ReadStartupMessage(clientConn)
	if err != nil {
		log.Printf("[client %s] startup read error: %v", remote, err)
		return
	}
	log.Printf("[client %s] startup: user=%s db=%s",
		remote, startup.Parameters["user"], startup.Parameters["database"])

	if err := sendAuthOK(clientConn); err != nil {
		log.Printf("[client %s] auth send error: %v", remote, err)
		return
	}

	inTransaction := false

	for {
		msg, err := protocol.ReadMessage(clientConn)
		if err != nil {
			if err != io.EOF {
				log.Printf("[client %s] read error: %v", remote, err)
			}
			return
		}

		if msg.Type == protocol.MsgTerminate {
			return
		}

		backendPool := rt.Route(msg, inTransaction)
		poolLabel := "primary"
		if backendPool != rt.Primary() {
			poolLabel = "replica"
		}

		if msg.Type == protocol.MsgQuery {
			sql := protocol.ExtractQueryText(msg.Payload)
			qt := protocol.ClassifyQuery(sql)
			metrics.Global.QueriesTotal.Inc()
			switch qt {
			case protocol.QueryRead:
				metrics.Global.QueriesRead.Inc()
			case protocol.QueryWrite:
				metrics.Global.QueriesWrite.Inc()
			default:
				metrics.Global.QueriesOther.Inc()
			}
			log.Printf("[client %s] %s → %s  %.80s", remote, qtName(qt), poolLabel, sql)
		}

		checkoutCtx, cancelCheckout := checkoutContext(cfg)
		bc, err := backendPool.Checkout(checkoutCtx)
		cancelCheckout()
		if err != nil {
			log.Printf("[client %s] pool checkout error: %v", remote, err)
			metrics.Global.PoolExhausted.Inc()
			sendError(clientConn, err.Error())
			continue
		}

		if err := protocol.WriteMessage(bc, msg); err != nil {
			log.Printf("[client %s] backend write error: %v", remote, err)
			metrics.Global.BackendErrors.Inc()
			backendPool.Discard(bc)
			sendError(clientConn, "backend write error")
			continue
		}

		txnStatus, err := forwardResponses(bc, clientConn)
		if err != nil {
			log.Printf("[client %s] forward error: %v", remote, err)
			metrics.Global.BackendErrors.Inc()
			backendPool.Discard(bc)
			return
		}

		// 'T' = in transaction, 'E' = failed transaction, 'I' = idle
		inTransaction = (txnStatus == 'T' || txnStatus == 'E')
		backendPool.Return(bc)
	}
}

func forwardResponses(backend *pool.BackendConn, client net.Conn) (byte, error) {
	for {
		msg, err := protocol.ReadMessage(backend)
		if err != nil {
			return 0, fmt.Errorf("reading backend response: %w", err)
		}
		if err := protocol.WriteMessage(client, msg); err != nil {
			return 0, fmt.Errorf("writing to client: %w", err)
		}
		if msg.Type == protocol.MsgReadyForQuery {
			if len(msg.Payload) > 0 {
				return msg.Payload[0], nil
			}
			return 'I', nil
		}
	}
}

func sendAuthOK(conn net.Conn) error {
	authOK := &protocol.Message{Type: protocol.MsgAuthRequest, Payload: []byte{0, 0, 0, 0}}
	if err := protocol.WriteMessage(conn, authOK); err != nil {
		return err
	}
	ps := &protocol.Message{
		Type:    protocol.MsgParameterStatus,
		Payload: append(append([]byte("client_encoding\x00"), []byte("UTF8")...), 0),
	}
	if err := protocol.WriteMessage(conn, ps); err != nil {
		return err
	}
	return protocol.WriteMessage(conn, &protocol.Message{Type: protocol.MsgReadyForQuery, Payload: []byte{'I'}})
}

func sendError(conn net.Conn, msg string) {
	var payload []byte
	payload = append(payload, 'S')
	payload = append(payload, []byte("ERROR\x00")...)
	payload = append(payload, 'M')
	payload = append(payload, []byte(msg)...)
	payload = append(payload, 0, 0)
	_ = protocol.WriteMessage(conn, &protocol.Message{Type: protocol.MsgErrorResponse, Payload: payload})
	_ = protocol.WriteMessage(conn, &protocol.Message{Type: protocol.MsgReadyForQuery, Payload: []byte{'I'}})
}

func loadConfig() *config.Config {
	if len(os.Args) > 1 {
		cfg, err := config.Load(os.Args[1])
		if err != nil {
			log.Fatalf("config load error: %v", err)
		}
		return cfg
	}
	log.Println("no config file given, using defaults (localhost:5432)")
	return config.Default()
}

func qtName(qt protocol.QueryType) string {
	switch qt {
	case protocol.QueryRead:
		return "READ "
	case protocol.QueryWrite:
		return "WRITE"
	default:
		return "OTHER"
	}
}

// checkoutContext returns a context for pool checkout. If pool_acquire_timeout_seconds is 0,
// only explicit cancellation applies (we rely on the pool's internal acquire timeout).
func checkoutContext(cfg *config.Config) (context.Context, context.CancelFunc) {
	if cfg.PoolAcquireTimeoutSeconds <= 0 {
		return context.WithCancel(context.Background())
	}
	return context.WithTimeout(context.Background(),
		time.Duration(cfg.PoolAcquireTimeoutSeconds)*time.Second)
}
