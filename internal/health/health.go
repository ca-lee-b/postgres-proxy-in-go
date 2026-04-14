// Package health implements a background goroutine doing TCP healthchecks
package health

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/ca-lee-b/postgres-proxy-go/internal/config"
)

type Status int

const (
	StatusUp   Status = iota
	StatusDown Status = iota
)

type Checker struct {
	mu       sync.RWMutex
	statuses map[string]Status // key is host:port
	interval time.Duration
}

func New(interval time.Duration) *Checker {
	return &Checker{
		statuses: make(map[string]Status),
		interval: interval,
	}
}

func (c *Checker) Start(primary config.BackendConfig, replicas []config.BackendConfig) {
	all := append([]config.BackendConfig{primary}, replicas...)
	for _, b := range all {
		go c.runLoop(b)
	}
}

func (c *Checker) runLoop(b config.BackendConfig) {
	key := fmt.Sprintf("%s:%d", b.Host, b.Port)
	for {
		up := c.probe(b)
		c.mu.Lock()
		old := c.statuses[key]
		if up {
			c.statuses[key] = StatusUp
		} else {
			c.statuses[key] = StatusDown
		}
		c.mu.Unlock()

		newStatus := StatusDown
		if up {
			newStatus = StatusUp
		}

		if old != newStatus {
			if up {
				log.Printf("[health] %s is UP", key)
			} else {
				log.Printf("[health] %s is DOWN", key)
			}
		}

		time.Sleep(c.interval)
	}
}

func (c *Checker) probe(b config.BackendConfig) bool {
	addr := fmt.Sprintf("%s:%d", b.Host, b.Port)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// SetDown marks a backend as down (used in tests)
func (c *Checker) SetDown(b config.BackendConfig) {
	key := fmt.Sprintf("%s:%d", b.Host, b.Port)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statuses[key] = StatusDown
}

// SetUp marks a backend as up (used in tests)
func (c *Checker) SetUp(b config.BackendConfig) {
	key := fmt.Sprintf("%s:%d", b.Host, b.Port)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statuses[key] = StatusUp
}

func (c *Checker) IsUp(b config.BackendConfig) bool {
	key := fmt.Sprintf("%s:%d", b.Host, b.Port)
	c.mu.RLock()
	defer c.mu.RUnlock()
	s, ok := c.statuses[key]
	if !ok {
		return true // assume up until proven otherwise
	}
	return s == StatusUp
}
