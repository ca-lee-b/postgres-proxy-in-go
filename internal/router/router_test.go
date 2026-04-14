package router

import (
	"testing"

	"github.com/ca-lee-b/postgres-proxy-go/internal/config"
	"github.com/ca-lee-b/postgres-proxy-go/internal/pool"
	"github.com/ca-lee-b/postgres-proxy-go/internal/protocol"
)

func TestRouting(t *testing.T) {
	cfg := &config.Config{
		PoolSize: 2,
		Primary:  config.BackendConfig{Host: "primary.db", Port: 5432},
		Replicas: []config.BackendConfig{
			{Host: "replica-0.db", Port: 5432},
			{Host: "replica-1.db", Port: 5432},
		},
	}
	rt := New(cfg)

	cases := []struct {
		sql           string
		inTransaction bool
		wantPrimary   bool
		reason        string
	}{
		// Reads go to replicas
		{"SELECT * FROM users", false, false, "plain SELECT → replica"},
		{"SELECT count(*) FROM orders", false, false, "aggregate → replica"},
		{"EXPLAIN SELECT * FROM users", false, false, "EXPLAIN → replica"},
		{"WITH cte AS (SELECT 1) SELECT * FROM cte", false, false, "CTE → replica"},

		// Writes go to primary
		{"INSERT INTO users VALUES (1,'bob')", false, true, "INSERT → primary"},
		{"UPDATE users SET active=true", false, true, "UPDATE → primary"},
		{"DELETE FROM sessions WHERE expired=true", false, true, "DELETE → primary"},
		{"CREATE INDEX idx_users_email ON users(email)", false, true, "DDL → primary"},

		// Write-intent reads MUST go to primary
		{"SELECT * FROM accounts WHERE id=1 FOR UPDATE", false, true, "SELECT FOR UPDATE → primary"},
		{"SELECT id FROM jobs FOR SHARE", false, true, "SELECT FOR SHARE → primary"},

		// Anything inside a transaction goes to primary (consistency)
		{"SELECT * FROM users", true, true, "SELECT inside txn → primary"},
		{"SELECT 1", true, true, "SELECT 1 inside txn → primary"},
	}

	for _, tc := range cases {
		msg := queryMsg(tc.sql)
		pool := rt.Route(msg, tc.inTransaction)
		gotPrimary := pool == rt.Primary()

		status := "✓"
		if gotPrimary != tc.wantPrimary {
			status = "✗ WRONG"
		}
		dest := "replica"
		if gotPrimary {
			dest = "primary"
		}
		t.Logf("%s → %-8s  [%s]  %q", status, dest, tc.reason, tc.sql)

		if gotPrimary != tc.wantPrimary {
			t.Errorf("Route(%q, inTxn=%v): got primary=%v want %v", tc.sql, tc.inTransaction, gotPrimary, tc.wantPrimary)
		}
	}
}

func Test_RR(t *testing.T) {
	cfg := &config.Config{
		PoolSize: 2,
		Primary:  config.BackendConfig{Host: "primary", Port: 5432},
		Replicas: []config.BackendConfig{
			{Host: "replica-0", Port: 5432},
			{Host: "replica-1", Port: 5432},
			{Host: "replica-2", Port: 5432},
		},
	}
	rt := New(cfg)

	// Send 9 reads — each replica should get exactly 3
	poolCounts := make(map[*pool.Pool]int)
	for i := 0; i < 9; i++ {
		p := rt.Route(queryMsg("SELECT 1"), false)
		poolCounts[p]++
	}

	t.Logf("primary pool: %d queries", poolCounts[rt.Primary()])
	for i, p := range rt.replicas {
		t.Logf("replica-%d pool: %d queries", i, poolCounts[p])
	}

	for i, p := range rt.replicas {
		if poolCounts[p] != 3 {
			t.Errorf("replica-%d got %d queries, want 3", i, poolCounts[p])
		}
	}
}

func Test_TransactionPinning(t *testing.T) {
	cfg := &config.Config{
		PoolSize: 2,
		Primary:  config.BackendConfig{Host: "primary", Port: 5432},
		Replicas: []config.BackendConfig{
			{Host: "replica-0", Port: 5432},
		},
	}
	rt := New(cfg)

	type step struct {
		sql         string
		inTxn       bool
		wantPrimary bool
	}

	// Simulate the proxy's inTransaction flag being set/cleared
	// (in main.go this is driven by the ReadyForQuery status byte)
	steps := []step{
		// Before transaction
		{"SELECT * FROM products", false, false}, // → replica (normal read)

		// Inside transaction (inTransaction=true because BEGIN was sent)
		{"BEGIN", false, true},                                  // → primary (BEGIN is a write)
		{"SELECT balance FROM accounts WHERE id=1", true, true}, // → primary! (in txn)
		{"UPDATE accounts SET balance=balance-100", true, true}, // → primary
		{"SELECT balance FROM accounts WHERE id=2", true, true}, // → primary! must see the update
		{"COMMIT", true, true},                                  // → primary

		// After transaction (inTransaction=false again)
		{"SELECT * FROM products", false, false}, // → replica again
	}

	for _, s := range steps {
		pool := rt.Route(queryMsg(s.sql), s.inTxn)
		gotPrimary := pool == rt.Primary()
		dest := "replica "
		if gotPrimary {
			dest = "primary"
		}
		inTxnStr := "      "
		if s.inTxn {
			inTxnStr = "in-txn"
		}
		t.Logf("[%s] → %s  %q", inTxnStr, dest, s.sql)
		if gotPrimary != s.wantPrimary {
			t.Errorf("Route(%q, inTxn=%v): got primary=%v want %v", s.sql, s.inTxn, gotPrimary, s.wantPrimary)
		}
	}
}

func Test_ExtendedQueryProtocol(t *testing.T) {
	cfg := &config.Config{
		PoolSize: 2,
		Primary:  config.BackendConfig{Host: "primary", Port: 5432},
		Replicas: []config.BackendConfig{
			{Host: "replica-0", Port: 5432},
		},
	}
	rt := New(cfg)

	// Extended query messages
	extendedMsgs := []struct {
		msgType byte
		name    string
	}{
		{protocol.MsgParse, "Parse (prepare statement)"},
		{protocol.MsgBind, "Bind (bind parameters)"},
		{protocol.MsgExecute, "Execute (run it)"},
		{protocol.MsgDescribe, "Describe (get column info)"},
		{protocol.MsgSync, "Sync (end of extended query)"},
	}

	for _, m := range extendedMsgs {
		msg := &protocol.Message{Type: m.msgType, Payload: []byte("SELECT $1")}
		pool := rt.Route(msg, false)
		gotPrimary := pool == rt.Primary()
		t.Logf("%-40s → primary=%v", m.name, gotPrimary)
		if !gotPrimary {
			t.Errorf("%s should route to primary, got replica", m.name)
		}
	}

	// ----- TRY THIS -----
	// Why can't we just inspect the SQL inside Parse and route reads to replicas?
	//
	// The problem is STATEMENT IDENTITY. After you Parse a statement, it gets
	// a name (e.g. "s1"). The subsequent Bind refers to it by name:
	//   Parse: "s1" = "SELECT * FROM users WHERE id=$1"
	//   Bind:  portal "p1" uses statement "s1" with params [42]
	//   Execute: run portal "p1"
	//
	// If Parse goes to replica-0 and Bind goes to replica-1, replica-1 doesn't
	// know about statement "s1". You'd need to track which statement names are
	// on which backend — that's what PgBouncer's "statement-level pooling" does,
	// and why it's complex. Routing everything to primary is the safe default.
}

func Test_ReplicaFallback(t *testing.T) {
	cfg := &config.Config{
		PoolSize: 2,
		Primary:  config.BackendConfig{Host: "primary", Port: 5432},
		Replicas: []config.BackendConfig{
			{Host: "replica-0", Port: 5432},
		},
	}
	rt := New(cfg)

	// Normal: read goes to replica
	pool := rt.Route(queryMsg("SELECT 1"), false)
	t.Logf("replica healthy   → primary=%v (want false)", pool == rt.Primary())
	if pool == rt.Primary() {
		t.Error("expected replica with healthy replica available")
	}

	// Mark replica as DOWN in the health checker
	rt.markDown(cfg.Replicas[0])
	pool = rt.Route(queryMsg("SELECT 1"), false)
	t.Logf("replica DOWN      → primary=%v (want true)", pool == rt.Primary())
	if pool != rt.Primary() {
		t.Error("expected primary fallback when all replicas are down")
	}

	// Bring it back UP
	rt.markUp(cfg.Replicas[0])
	pool = rt.Route(queryMsg("SELECT 1"), false)
	t.Logf("replica back UP   → primary=%v (want false)", pool == rt.Primary())
	if pool == rt.Primary() {
		t.Error("expected replica after recovery")
	}
}

func queryMsg(sql string) *protocol.Message {
	return &protocol.Message{
		Type:    protocol.MsgQuery,
		Payload: append([]byte(sql), 0),
	}
}

// Expose helpers on the router so tests can control health state.
func (r *Router) markDown(b config.BackendConfig) { r.checker.SetDown(b) }
func (r *Router) markUp(b config.BackendConfig)   { r.checker.SetUp(b) }
