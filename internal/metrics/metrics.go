// Package metrics exposes a simple HTTP endpoint at /metrics and /healthz
// so you can observe the proxy without connecting to it via Postgres.
package metrics

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

// Counter is a simple monotonically-increasing counter.
type Counter struct{ n atomic.Int64 }

func (c *Counter) Inc()         { c.n.Add(1) }
func (c *Counter) Add(d int64)  { c.n.Add(d) }
func (c *Counter) Value() int64 { return c.n.Load() }

// Counters holds all proxy-wide metrics.
type Counters struct {
	QueriesTotal  Counter
	QueriesRead   Counter
	QueriesWrite  Counter
	QueriesOther  Counter
	ClientsTotal  Counter
	ClientsActive Counter
	PoolExhausted Counter
	BackendErrors Counter
	startTime     time.Time
}

var Global = &Counters{startTime: time.Now()}

// PoolStatsFn is a callback to get pool stats at report time.
type PoolStatsFn func() []PoolStat

type PoolStat struct {
	Label string `json:"label"`
	Total int    `json:"total"`
	InUse int    `json:"in_use"`
}

// StartServer starts the HTTP metrics server on addr (e.g. ":9090").
func StartServer(addr string, poolStats PoolStatsFn) {
	mux := http.NewServeMux()

	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		stats := poolStats()
		report := map[string]any{
			"uptime_seconds": int(time.Since(Global.startTime).Seconds()),
			"queries_total":  Global.QueriesTotal.Value(),
			"queries_read":   Global.QueriesRead.Value(),
			"queries_write":  Global.QueriesWrite.Value(),
			"queries_other":  Global.QueriesOther.Value(),
			"clients_total":  Global.ClientsTotal.Value(),
			"clients_active": Global.ClientsActive.Value(),
			"pool_exhausted": Global.PoolExhausted.Value(),
			"backend_errors": Global.BackendErrors.Value(),
			"pools":          stats,
		}
		_ = json.NewEncoder(w).Encode(report)
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "ok")
	})

	log.Printf("[metrics] HTTP server listening on %s", addr)
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("[metrics] server error: %v", err)
		}
	}()
}
