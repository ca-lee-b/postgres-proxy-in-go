// SCRAM-SHA-256 client authentication for Postgres (RFC 5802)
// This took forever
//
// Postgres SASL framing docs:
//
//	https://www.postgresql.org/docs/current/sasl-authentication.html
//
// Auth message flow (authType codes from 'R' messages):
//
//	10 AuthenticationSASL      — server lists mechanisms
//	11 AuthenticationSASLContinue — server-first-message
//	12 AuthenticationSASLFinal    — server-final-message (contains v=)
//	 0 AuthenticationOk           — returned to outer doStartup loop
package pool

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"strings"

	"golang.org/x/crypto/pbkdf2"

	"github.com/ca-lee-b/postgres-proxy-go/internal/config"
	"github.com/ca-lee-b/postgres-proxy-go/internal/protocol"
)

const scramMechanism = "SCRAM-SHA-256"

// doSCRAM drives the full SCRAM-SHA-256 exchange starting from the moment
// doStartup has just read the AuthenticationSASL (10) message
// saslPayload is msg.Payload[4:] — the mechanism list (null-terminated strings)
func doSCRAM(conn net.Conn, cfg config.BackendConfig, saslPayload []byte) error {
	if !containsMechanism(saslPayload, scramMechanism) {
		return fmt.Errorf("scram: server does not offer %s", scramMechanism)
	}

	// --- Step 1: build client-first-message ---
	clientNonce, err := generateNonce(18)
	if err != nil {
		return fmt.Errorf("scram: nonce: %w", err)
	}
	return doSCRAMWithNonce(conn, cfg.Password, clientNonce)
}

// doSCRAMWithNonce is the testable core — accepts an explicit nonce so tests
// can drive it deterministically without a real network connection
func doSCRAMWithNonce(conn net.Conn, password, clientNonce string) error {
	// GS2 header: n = no channel binding, anonymous authzid.
	const gs2Header = "n,,"
	clientFirstBare := "n=,r=" + clientNonce
	clientFirstMsg := gs2Header + clientFirstBare

	// --- Step 2: send SASLInitialResponse ---
	if err := sendSASLInitialResponse(conn, clientFirstMsg); err != nil {
		return err
	}

	// --- Step 3: read AuthenticationSASLContinue (11) ---
	msg, err := protocol.ReadMessage(conn)
	if err != nil {
		return fmt.Errorf("scram: read SASLContinue: %w", err)
	}
	if msg.Type != protocol.MsgAuthRequest {
		return fmt.Errorf("scram: expected auth message, got %q", msg.Type)
	}
	authType11 := int32(binary.BigEndian.Uint32(msg.Payload[:4]))
	if authType11 != 11 {
		return fmt.Errorf("scram: expected SASLContinue (11), got %d", authType11)
	}
	serverFirst := string(msg.Payload[4:])

	// --- Step 4: parse server-first-message ---
	serverNonce, saltB64, iterations, err := parseServerFirst(serverFirst, clientNonce)
	if err != nil {
		return fmt.Errorf("scram: parse server-first: %w", err)
	}

	salt, err := base64.StdEncoding.DecodeString(saltB64)
	if err != nil {
		return fmt.Errorf("scram: decode salt: %w", err)
	}

	// --- Step 5: compute cryptographic values ---
	channelBinding := base64.StdEncoding.EncodeToString([]byte(gs2Header))
	clientFinalWithoutProof := "c=" + channelBinding + ",r=" + serverNonce

	clientProof, serverSignature, err := scramProofs(
		password, clientFirstBare, serverFirst, salt, iterations, clientFinalWithoutProof,
	)
	if err != nil {
		return err
	}

	clientFinalMsg := clientFinalWithoutProof + ",p=" + base64.StdEncoding.EncodeToString(clientProof)

	// --- Step 6: send SASLResponse ---
	if err := sendSASLResponse(conn, clientFinalMsg); err != nil {
		return err
	}

	// --- Step 7: read AuthenticationSASLFinal (12) ---
	msg, err = protocol.ReadMessage(conn)
	if err != nil {
		return fmt.Errorf("scram: read SASLFinal: %w", err)
	}
	if msg.Type != protocol.MsgAuthRequest {
		return fmt.Errorf("scram: expected auth message, got %q", msg.Type)
	}
	authType12 := int32(binary.BigEndian.Uint32(msg.Payload[:4]))
	if authType12 != 12 {
		return fmt.Errorf("scram: expected SASLFinal (12), got %d", authType12)
	}
	serverFinal := string(msg.Payload[4:])

	// --- Step 8: verify server signature ---
	if err := verifyServerFinal(serverFinal, serverSignature); err != nil {
		return fmt.Errorf("scram: server verification failed: %w", err)
	}

	// AuthenticationOk (0) will be read by the outer doStartup loop.
	return nil
}

// scramProofs computes ClientProof and ServerSignature given all message inputs
// clientFinalWithoutProof is already constructed by the caller
func scramProofs(password, clientFirstBare, serverFirst string, salt []byte, iterations int, clientFinalWithoutProof string) (clientProof, serverSignature []byte, err error) {
	saltedPassword := pbkdf2.Key([]byte(password), salt, iterations, 32, sha256.New)

	clientKey := hmacSHA256(saltedPassword, []byte("Client Key"))
	storedKey := sha256Digest(clientKey)
	serverKey := hmacSHA256(saltedPassword, []byte("Server Key"))

	authMessage := clientFirstBare + "," + serverFirst + "," + clientFinalWithoutProof

	clientSignature := hmacSHA256(storedKey, []byte(authMessage))
	clientProof = xorBytes(clientKey, clientSignature)
	serverSignature = hmacSHA256(serverKey, []byte(authMessage))
	return clientProof, serverSignature, nil
}

// sendSASLInitialResponse sends the first PasswordMessage ('p') of the SASL exchange.
// Wire format: mechanism\0 + int32(len(client-first)) + client-first (no null terminator).
func sendSASLInitialResponse(conn net.Conn, clientFirstMsg string) error {
	mechanismBytes := append([]byte(scramMechanism), 0)
	msgLen := len(clientFirstMsg)

	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(msgLen))

	payload := append(mechanismBytes, lenBytes...)
	payload = append(payload, []byte(clientFirstMsg)...)

	return protocol.WriteMessage(conn, &protocol.Message{
		Type:    protocol.MsgPassword,
		Payload: payload,
	})
}

// sendSASLResponse sends the client-final-message as a PasswordMessage ('p')
func sendSASLResponse(conn net.Conn, clientFinalMsg string) error {
	return protocol.WriteMessage(conn, &protocol.Message{
		Type:    protocol.MsgPassword,
		Payload: []byte(clientFinalMsg),
	})
}

// parseServerFirst extracts r=, s=, i= from the server-first-message
// Also validates that the server nonce starts with the client nonce.
func parseServerFirst(serverFirst, clientNonce string) (serverNonce, saltB64 string, iterations int, err error) {
	parts := strings.Split(serverFirst, ",")
	attrs := make(map[string]string, len(parts))
	for _, p := range parts {
		if len(p) < 2 || p[1] != '=' {
			return "", "", 0, fmt.Errorf("malformed attribute %q", p)
		}
		attrs[string(p[0])] = p[2:]
	}

	serverNonce = attrs["r"]
	if serverNonce == "" {
		return "", "", 0, fmt.Errorf("missing r= attribute")
	}
	if !strings.HasPrefix(serverNonce, clientNonce) {
		return "", "", 0, fmt.Errorf("server nonce does not start with client nonce")
	}

	saltB64 = attrs["s"]
	if saltB64 == "" {
		return "", "", 0, fmt.Errorf("missing s= attribute")
	}

	iterStr := attrs["i"]
	if iterStr == "" {
		return "", "", 0, fmt.Errorf("missing i= attribute")
	}
	iterations, err = strconv.Atoi(iterStr)
	if err != nil || iterations < 1 {
		return "", "", 0, fmt.Errorf("invalid iteration count %q", iterStr)
	}

	return serverNonce, saltB64, iterations, nil
}

// verifyServerFinal checks that v= in the server-final-message matches our expected signature.
func verifyServerFinal(serverFinal string, expectedSignature []byte) error {
	if !strings.HasPrefix(serverFinal, "v=") {
		// RFC 5802 also allows e= for an error.
		if strings.HasPrefix(serverFinal, "e=") {
			return fmt.Errorf("server returned error: %s", serverFinal[2:])
		}
		return fmt.Errorf("unexpected server-final format: %q", serverFinal)
	}
	gotSig, err := base64.StdEncoding.DecodeString(serverFinal[2:])
	if err != nil {
		return fmt.Errorf("decode server signature: %w", err)
	}
	if !hmac.Equal(gotSig, expectedSignature) {
		return fmt.Errorf("server signature mismatch")
	}
	return nil
}

// containsMechanism scans a null-terminated list of mechanism names.
func containsMechanism(payload []byte, mechanism string) bool {
	target := []byte(mechanism)
	i := 0
	for i < len(payload) {
		j := i
		for j < len(payload) && payload[j] != 0 {
			j++
		}
		if string(payload[i:j]) == string(target) {
			return true
		}
		i = j + 1
	}
	return false
}

func generateNonce(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawStdEncoding.EncodeToString(b), nil
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func sha256Digest(data []byte) []byte {
	d := sha256.Sum256(data)
	return d[:]
}

func xorBytes(a, b []byte) []byte {
	out := make([]byte, len(a))
	for i := range a {
		out[i] = a[i] ^ b[i]
	}
	return out
}
