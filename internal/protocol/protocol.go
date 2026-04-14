// Package protocol implements postgres wire protocol v3
// Thankfully the protocol is pretty simple lol
// Messages have a 1 byte type tag, 4 byte length, and then the payload
package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
)

const (
	MsgQuery     byte = 'Q' // Simple query
	MsgParse     byte = 'P' // Extended parse
	MsgBind      byte = 'B' // Extended bind
	MsgExecute   byte = 'E' // Extended execute
	MsgDescribe  byte = 'D' // Describe
	MsgSync      byte = 'S' // Sync
	MsgTerminate byte = 'X'
	MsgPassword  byte = 'P' // Password response

	MsgAuthRequest     byte = 'R'
	MsgRowDescription  byte = 'T'
	MsgDataRow         byte = 'D'
	MsgCommandComplete byte = 'C'
	MsgReadyForQuery   byte = 'Z'
	MsgErrorResponse   byte = 'E'
	MsgNoticeResponse  byte = 'N'
	MsgParameterStatus byte = 'S'
	MsgBackendKeyData  byte = 'K'
	MsgNoData          byte = 'n'
	MsgParseComplete   byte = '1'
	MsgBindComplete    byte = '2'
	MsgEmptyQuery      byte = 'I'
)

// Message is a raw wire protocol message
type Message struct {
	Type    byte
	Payload []byte
}

// StartupMessage is the first message a client sends
type StartupMessage struct {
	ProtocolVersion int
	Parameters      map[string]string
}

// ReadMessage reads one message post-startup
func ReadMessage(r io.Reader) (*Message, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	msgType := header[0]

	length := int(binary.BigEndian.Uint32(header[1:5]))
	if length < 4 {
		return nil, fmt.Errorf("invalid message length %d", length)
	}

	payload := make([]byte, length-4) // note: length includes the 4 byte length field itself
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	return &Message{
		Type:    msgType,
		Payload: payload,
	}, nil
}

// ReadStartupMessage reads the special startup message
func ReadStartupMessage(r io.Reader) (*StartupMessage, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil, err
	}

	length := int(binary.BigEndian.Uint32(lenBuf))
	if length < 8 || length > 10000 {
		return nil, fmt.Errorf("invalid startup length %d", length)
	}

	versionBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, versionBuf); err != nil {
		return nil, err
	}
	version := int(binary.BigEndian.Uint32(versionBuf))

	body := make([]byte, length-8)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}

	params := map[string]string{}
	rest := body

	for len(rest) > 1 {
		k, remaining, err := readCString(rest)
		if err != nil || k == "" {
			break
		}

		v, remaining, err := readCString(remaining)
		if err != nil {
			break
		}

		params[k] = v
		rest = remaining
	}

	return &StartupMessage{
		ProtocolVersion: version,
		Parameters:      params,
	}, nil
}

// WriteMessage serializes a message to wire
func WriteMessage(w io.Writer, msg *Message) error {
	buf := make([]byte, 5+len(msg.Payload)) // remember allocate for type and length fields
	buf[0] = msg.Type

	binary.BigEndian.PutUint32(buf[1:5], uint32(len(msg.Payload)+4))
	copy(buf[5:], msg.Payload)

	_, err := w.Write(buf)
	return err
}

// readCString reads key/value strings
func readCString(b []byte) (string, []byte, error) {
	for i, c := range b {
		if c == 0 {
			return string(b[:i]), b[i+1:], nil
		}
	}

	return "", nil, fmt.Errorf("no null terminator")
}

type QueryType int

const (
	QueryRead  QueryType = iota // safe to send to a replica
	QueryWrite                  // must go to primary
	QueryOther                  // DDL, SET, SHOW, etc. send to primary to be safe
)

var writePrefixes = []string{
	"insert", "update", "delete", "truncate",
	"create", "drop", "alter", "grant", "revoke",
	"begin", "start", "commit", "rollback", "savepoint",
	"call", "do",
}

var readOnlyPrefixes = []string{"select", "show", "explain", "with"}

// ExtractQueryText pulls the query string out of a 'Q' (simple query) message
func ExtractQueryText(payload []byte) string {
	// Simple query payload is a null-terminated string
	if len(payload) == 0 {
		return ""
	}
	end := len(payload)
	if payload[end-1] == 0 {
		end--
	}
	return string(payload[:end])
}

// ClassifyQuery returns whether a query is a read, write, or other.
// There's probably a better way to do this
func ClassifyQuery(sql string) QueryType {
	trimmed := strings.TrimSpace(strings.ToLower(sql))

	// SELECT FOR UPDATE / SHARE must go to primary
	if strings.HasPrefix(trimmed, "select") {
		if strings.Contains(trimmed, "for update") || strings.Contains(trimmed, "for share") {
			return QueryWrite
		}
		return QueryRead
	}

	for _, p := range readOnlyPrefixes {
		if strings.HasPrefix(trimmed, p) {
			return QueryRead
		}
	}

	for _, p := range writePrefixes {
		if strings.HasPrefix(trimmed, p) {
			return QueryWrite
		}
	}

	return QueryOther
}
