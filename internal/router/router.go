// Package router chooses the appropriate backend for a query
package router

import (
	"fmt"
	"sync/atomic"

	"github.com/ca-lee-b/postgres-proxy-go/internal/config"
	"github.com/ca-lee-b/postgres-proxy-go/internal/health"
	"github.com/ca-lee-b/postgres-proxy-go/internal/pool"
	"github.com/ca-lee-b/postgres-proxy-go/internal/protocol"
)

type Router struct {
	primary     *pool.Pool
	replicas    []*pool.Pool
	replicaCfgs []config.BackendConfig
	checker     *health.Checker
	rrCounter   atomic.Uint64 // round-robin for replica selection
}

func New(config *config.Config) *Router {
	primary := pool.New(config.Primary, config.PoolSize, "primary")

	replicas := make([]*pool.Pool, len(config.Replicas))
	for i, r := range config.Replicas {
		replicas[i] = pool.New(r, config.PoolSize, fmt.Sprintf("replica-%d", i))
	}

	checker := health.New(0) // caller can set interval and start
	return &Router{
		primary:     primary,
		replicas:    replicas,
		replicaCfgs: config.Replicas,
		checker:     checker,
	}
}

// NewWithChecker creates a health checker (so the proxy can share it)
func NewWithChecker(cfg *config.Config, checker *health.Checker) *Router {
	r := New(cfg)
	r.checker = checker
	return r
}

// Route returns the appropriate pool for this message
func (r *Router) Route(msg *protocol.Message, inTransaction bool) *pool.Pool {
	if inTransaction {
		return r.primary
	}

	if msg.Type != protocol.MsgQuery {
		return r.primary
	}

	sql := protocol.ExtractQueryText(msg.Payload)
	qt := protocol.ClassifyQuery(sql)

	if qt == protocol.QueryRead && len(r.replicas) > 0 {
		if p := r.pickReplica(); p != nil {
			return p
		}
	}

	return r.primary
}

// pickReplica selects a healthy replica using round-robin
func (r *Router) pickReplica() *pool.Pool {
	n := uint64(len(r.replicas))
	if n == 0 {
		return nil
	}
	// Try each replica once starting from the next in rotation
	start := r.rrCounter.Add(1) - 1
	for i := uint64(0); i < n; i++ {
		idx := (start + i) % n
		if r.checker.IsUp(r.replicaCfgs[idx]) {
			return r.replicas[idx]
		}
	}
	return nil // all replicas down — caller falls back to primary
}

// Primary returns the primary pool directly (used for writes / transactions).
func (r *Router) Primary() *pool.Pool { return r.primary }

func (r *Router) AllPools() []*pool.Pool {
	all := make([]*pool.Pool, 0, 1+len(r.replicas))
	all = append(all, r.primary)
	all = append(all, r.replicas...)
	return all
}
