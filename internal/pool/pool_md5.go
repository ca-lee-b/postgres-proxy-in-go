package pool

import (
	"crypto/md5"
	"fmt"
	"net"

	"github.com/ca-lee-b/postgres-proxy-go/internal/protocol"
)

// md5AuthResponse computes the Postgres MD5 password hash
//
//	"md5" + hex( md5( hex( md5(password + user) ) + salt ) )
//
// The salt is the 4-byte random value from the AuthenticationMD5Password message
func md5AuthResponse(user, password string, salt []byte) string {
	inner := md5.Sum([]byte(password + user))
	innerHex := fmt.Sprintf("%x", inner)

	outer := md5.Sum([]byte(innerHex + string(salt)))
	return "md5" + fmt.Sprintf("%x", outer)
}

// sendMD5Password sends the PasswordMessage response for MD5 auth
func sendMD5Password(conn net.Conn, user, password string, salt []byte) error {
	hashed := md5AuthResponse(user, password, salt)
	payload := append([]byte(hashed), 0) // null-terminated string
	return protocol.WriteMessage(conn, &protocol.Message{
		Type:    protocol.MsgPassword,
		Payload: payload,
	})
}
