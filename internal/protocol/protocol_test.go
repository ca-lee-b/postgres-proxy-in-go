package protocol

import (
	"bytes"
	"testing"
)

// Test round trip of WriteMessage -> ReadMessage
func Test_Message(t *testing.T) {
	original := &Message{
		Type:    MsgQuery,
		Payload: []byte("SELECT 1\x00"), // null terminate
	}

	buf := &bytes.Buffer{}

	if err := WriteMessage(buf, original); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	wire := buf.Bytes()
	t.Logf("wire bytes (%d total): %v", len(wire), wire)
	t.Logf("   type byte : %c (0x%02x)", wire[0], wire[0])
	t.Logf("   length    : %d", int(wire[1])<<24|int(wire[2])<<16|int(wire[3])<<8|int(wire[4]))
	t.Logf("   payload   : %q", wire[5:])

	received, err := ReadMessage(buf)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}

	if received.Type != original.Type {
		t.Errorf("type: got %c want %c", received.Type, original.Type)
	}

	if !bytes.Equal(received.Payload, original.Payload) {
		t.Errorf("payload: got %q want %q", received.Payload, original.Payload)
	}
}

func Test_StartupMessage(t *testing.T) {
	params := buildTestStartupParams("test", "mydb")
	totalLen := 4 + 4 + len(params)

	raw := make([]byte, totalLen)
	raw[0] = byte(totalLen >> 24)
	raw[1] = byte(totalLen >> 16)
	raw[2] = byte(totalLen >> 8)
	raw[3] = byte(totalLen)
	raw[4] = 0x00
	raw[5] = 0x03
	raw[6] = 0x00
	raw[7] = 0x00 // v3.0
	copy(raw[8:], params)

	t.Logf("startup packet (%d bytes): %v", len(raw), raw)
	t.Logf("as string: %q", raw)

	msg, err := ReadStartupMessage(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("ReadStartupMessage: %v", err)
	}

	t.Logf("   parsed version: 0x%08x", msg.ProtocolVersion)
	t.Logf("   parsed length : %d", totalLen)
	t.Logf("   parsed params : %v", msg.Parameters)

	if msg.Parameters["user"] != "test" {
		t.Errorf("user: got %q want %q", msg.Parameters["user"], "test")
	}
	if msg.Parameters["database"] != "mydb" {
		t.Errorf("database: got %q want %q", msg.Parameters["database"], "mydb")
	}
}

// buildTestStartupParams builds the key=value null-terminated parameter block
// for a Postgres startup message (Mirrors the private pool.buildStartupParams.)
func buildTestStartupParams(user, database string) []byte {
	var b []byte
	add := func(k, v string) {
		b = append(b, []byte(k)...)
		b = append(b, 0)
		b = append(b, []byte(v)...)
		b = append(b, 0)
	}
	add("user", user)
	add("database", database)
	add("application_name", "proxy-test")
	b = append(b, 0) // terminator
	return b
}
