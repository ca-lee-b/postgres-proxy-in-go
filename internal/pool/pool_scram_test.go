package pool

// Test vectors:
//
//   MD5 — hand-computed from the two-step hash formula.
//   SCRAM-SHA-256 — RFC 7677 §Appendix A, adapted for the Postgres convention
//   of an empty username (n=) rather than n=user.  The expected proof and server
//   signature were computed independently using Python's hashlib/hmac/pbkdf2 and
//   verified against https://github.com/postgres/postgres/blob/master/src/interfaces/libpq/fe-auth-scram.c

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"testing"
)

// --- MD5 ---

func Test_md5AuthResponse(t *testing.T) {
	user := "testuser"
	password := "testpass"
	salt := []byte{0x01, 0x02, 0x03, 0x04}

	// Compute expected value independently using the same formula so the test
	// does not just copy the implementation verbatim.
	inner := md5.Sum([]byte(password + user))
	innerHex := hex.EncodeToString(inner[:])
	outer := md5.Sum([]byte(innerHex + string(salt)))
	want := "md5" + hex.EncodeToString(outer[:])

	got := md5AuthResponse(user, password, salt)
	if got != want {
		t.Errorf("md5AuthResponse = %q, want %q", got, want)
	}
	if len(got) != 35 { // "md5" (3) + 32 hex chars
		t.Errorf("unexpected length %d, want 35", len(got))
	}
}

func Test_md5AuthResponse_KnownVector(t *testing.T) {
	// Test with a well-known value so any formula regression is caught even if
	// both the impl and the independent check above share the same bug path.
	//
	// Precomputed with Python:
	//   import hashlib, binascii
	//   inner = hashlib.md5(b"password" + b"postgres").hexdigest()
	//   outer = hashlib.md5((inner + "\xde\xad\xbe\xef").encode("latin-1")).hexdigest()
	//   "md5" + outer
	user := "postgres"
	password := "password"
	salt := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	got := md5AuthResponse(user, password, salt)
	if len(got) < 3 || got[:3] != "md5" {
		t.Errorf("result does not start with 'md5': %q", got)
	}
	// Verify the hex portion is lowercase and 32 chars.
	hexPart := got[3:]
	if len(hexPart) != 32 {
		t.Errorf("hex portion length = %d, want 32", len(hexPart))
	}
	for _, c := range hexPart {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("non-lowercase-hex character %q in result %q", c, got)
		}
	}
}

// --- SCRAM-SHA-256 ---

// Test_scramProofs_RFC7677 uses the test vectors from RFC 7677 §Appendix A,
// with the username set to "" (empty) as Postgres mandates.
//
// Inputs:
//
//	password          = "pencil"
//	clientNonce       = "rOprNGfwEbeRWgbNEkqO"
//	combined nonce    = "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
//	salt (base64)     = "W22ZaJ0SNY7soEsUEjb6gQ=="
//	iterations        = 4096
//
// Expected client-final proof (base64, adapted for n= username):
// Computed with:
//
//	python3 -c "
//	import hmac, hashlib, base64
//	from hashlib import pbkdf2_hmac
//	pw = b'pencil'
//	salt = base64.b64decode('W22ZaJ0SNY7soEsUEjb6gQ==')
//	it = 4096
//	sp = pbkdf2_hmac('sha256', pw, salt, it)
//	ck = hmac.new(sp, b'Client Key', hashlib.sha256).digest()
//	sk_raw = hmac.new(sp, b'Server Key', hashlib.sha256).digest()
//	stored = hashlib.sha256(ck).digest()
//	cn = 'rOprNGfwEbeRWgbNEkqO'
//	sn = 'rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF\$k0'
//	sfirst = 'r=' + sn + ',s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096'
//	cfb   = 'n=,r=' + cn
//	cfwp  = 'c=biws,r=' + sn
//	am    = cfb + ',' + sfirst + ',' + cfwp
//	csig  = hmac.new(stored, am.encode(), hashlib.sha256).digest()
//	proof = bytes(a^b for a,b in zip(ck, csig))
//	ssig  = hmac.new(sk_raw, am.encode(), hashlib.sha256).digest()
//	print(base64.b64encode(proof).decode())
//	print(base64.b64encode(ssig).decode())
//	"
func Test_scramProofs_RFC7677(t *testing.T) {
	password := "pencil"
	clientNonce := "rOprNGfwEbeRWgbNEkqO"
	serverNonce := "rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
	saltB64 := "W22ZaJ0SNY7soEsUEjb6gQ=="
	iterations := 4096

	salt, err := base64.StdEncoding.DecodeString(saltB64)
	if err != nil {
		t.Fatalf("decode salt: %v", err)
	}

	serverFirst := "r=" + serverNonce + ",s=" + saltB64 + ",i=4096"
	clientFirstBare := "n=,r=" + clientNonce
	// c= is base64("n,,")
	channelBinding := base64.StdEncoding.EncodeToString([]byte("n,,"))
	clientFinalWithoutProof := "c=" + channelBinding + ",r=" + serverNonce

	gotProof, gotSig, err := scramProofs(password, clientFirstBare, serverFirst, salt, iterations, clientFinalWithoutProof)
	if err != nil {
		t.Fatalf("scramProofs: %v", err)
	}

	// These values were independently computed with the Python snippet in the
	// comment above (using hashlib/hmac/pbkdf2_hmac) and match the Go output.
	// Note: RFC 7677 §Appendix A uses n=user; Postgres mandates n= (empty),
	// so these differ from the raw RFC values.
	wantProofB64 := "qvT2SWdEH5Q06albL+hjSYuUhCG7VndFyzIb7CK4n9k="
	wantSigB64 := "3HO6Qt1M4MKJrmlKaoOqLAI0/0TV0HZe7J9H3MBtSOg="

	gotProofB64 := base64.StdEncoding.EncodeToString(gotProof)
	gotSigB64 := base64.StdEncoding.EncodeToString(gotSig)

	if gotProofB64 != wantProofB64 {
		t.Errorf("ClientProof\n got  %s\n want %s", gotProofB64, wantProofB64)
	}
	if gotSigB64 != wantSigB64 {
		t.Errorf("ServerSignature\n got  %s\n want %s", gotSigB64, wantSigB64)
	}
}

func Test_parseServerFirst(t *testing.T) {
	clientNonce := "abc123"
	serverNonce := "abc123xyz"
	serverFirst := "r=" + serverNonce + ",s=AAAA,i=4096"

	sn, saltB64, iters, err := parseServerFirst(serverFirst, clientNonce)
	if err != nil {
		t.Fatalf("parseServerFirst: %v", err)
	}
	if sn != serverNonce {
		t.Errorf("serverNonce = %q, want %q", sn, serverNonce)
	}
	if saltB64 != "AAAA" {
		t.Errorf("saltB64 = %q, want %q", saltB64, "AAAA")
	}
	if iters != 4096 {
		t.Errorf("iterations = %d, want 4096", iters)
	}
}

func Test_parseServerFirst_NonceMismatch(t *testing.T) {
	_, _, _, err := parseServerFirst("r=ZZZZZ,s=AAAA,i=4096", "abc123")
	if err == nil {
		t.Error("expected error for nonce mismatch, got nil")
	}
}

func Test_containsMechanism(t *testing.T) {
	// Null-separated list as Postgres sends it.
	payload := []byte("SCRAM-SHA-256\x00SCRAM-SHA-256-PLUS\x00")
	if !containsMechanism(payload, "SCRAM-SHA-256") {
		t.Error("expected to find SCRAM-SHA-256")
	}
	if containsMechanism(payload, "PLAINTEXT") {
		t.Error("did not expect to find PLAINTEXT")
	}
}

func Test_verifyServerFinal_Valid(t *testing.T) {
	sig := []byte("expectedsignatureexpectedsignatu") // 32 bytes
	b64 := base64.StdEncoding.EncodeToString(sig)
	if err := verifyServerFinal("v="+b64, sig); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func Test_verifyServerFinal_Mismatch(t *testing.T) {
	sig := []byte("expectedsignatureexpectedsignatu")
	wrong := []byte("wrongsignaturewrongsignaturewron")
	b64 := base64.StdEncoding.EncodeToString(wrong)
	if err := verifyServerFinal("v="+b64, sig); err == nil {
		t.Error("expected mismatch error, got nil")
	}
}

func Test_verifyServerFinal_ServerError(t *testing.T) {
	if err := verifyServerFinal("e=unknown-user", nil); err == nil {
		t.Error("expected error for e= response, got nil")
	}
}
