package irc

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

type recordingConn struct {
	writes []string
}

func (c *recordingConn) Read(_ []byte) (int, error) { return 0, io.EOF }
func (c *recordingConn) Write(b []byte) (int, error) {
	c.writes = append(c.writes, string(b))
	return len(b), nil
}
func (c *recordingConn) Close() error                       { return nil }
func (c *recordingConn) LocalAddr() net.Addr                { return nil }
func (c *recordingConn) RemoteAddr() net.Addr               { return nil }
func (c *recordingConn) SetDeadline(_ time.Time) error      { return nil }
func (c *recordingConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *recordingConn) SetWriteDeadline(_ time.Time) error { return nil }

func TestDH1080AutoExchangeQueuesAndFlushesPM(t *testing.T) {
	aliceConn := &recordingConn{}
	bobConn := &recordingConn{}

	alice := NewBot("irc.example.net", 6697, "AliceBot", "alice", "Alice")
	alice.Conn = aliceConn
	alice.Connected = true
	alice.SetAutoExchange(true)

	bob := NewBot("irc.example.net", 6697, "BobBot", "bob", "Bob")
	bob.Conn = bobConn
	bob.Connected = true
	bob.SetAutoExchange(true)

	if err := alice.SendMessage("BobBot", "hello from alice"); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if len(aliceConn.writes) != 1 {
		t.Fatalf("expected one DH1080 init write, got %d", len(aliceConn.writes))
	}
	initLine := strings.TrimSpace(aliceConn.writes[0])
	if !strings.HasPrefix(initLine, "NOTICE BobBot :DH1080_INIT ") {
		t.Fatalf("unexpected init line: %q", initLine)
	}
	initMsg := strings.TrimPrefix(initLine, "NOTICE BobBot :")

	handled, err := bob.HandleKeyExchange("AliceBot", "BobBot", initMsg)
	if err != nil {
		t.Fatalf("bob HandleKeyExchange init: %v", err)
	}
	if !handled {
		t.Fatalf("expected bob to handle DH1080_INIT")
	}
	if len(bobConn.writes) != 1 {
		t.Fatalf("expected one DH1080 finish write, got %d", len(bobConn.writes))
	}
	finishLine := strings.TrimSpace(bobConn.writes[0])
	if !strings.HasPrefix(finishLine, "NOTICE AliceBot :DH1080_FINISH ") {
		t.Fatalf("unexpected finish line: %q", finishLine)
	}
	finishMsg := strings.TrimPrefix(finishLine, "NOTICE AliceBot :")

	handled, err = alice.HandleKeyExchange("BobBot", "AliceBot", finishMsg)
	if err != nil {
		t.Fatalf("alice HandleKeyExchange finish: %v", err)
	}
	if !handled {
		t.Fatalf("expected alice to handle DH1080_FINISH")
	}
	if len(aliceConn.writes) < 2 {
		t.Fatalf("expected flushed encrypted PM after finish, got %d writes", len(aliceConn.writes))
	}
	privmsg := strings.TrimSpace(aliceConn.writes[1])
	if !strings.HasPrefix(privmsg, "PRIVMSG BobBot :+OK *") {
		t.Fatalf("unexpected encrypted PRIVMSG: %q", privmsg)
	}
	ciphertext := strings.TrimPrefix(privmsg, "PRIVMSG BobBot :")
	plain, ok := bob.DecryptIncomingMessage("AliceBot", "BobBot", ciphertext)
	if !ok {
		t.Fatalf("expected bob to decrypt the negotiated PM")
	}
	if plain != "hello from alice" {
		t.Fatalf("decrypted PM = %q, want %q", plain, "hello from alice")
	}
}

func TestDH1080CtxNegotiatesSameKey(t *testing.T) {
	alice, err := NewDH1080Ctx("CBC")
	if err != nil {
		t.Fatalf("NewDH1080Ctx alice: %v", err)
	}
	bob, err := NewDH1080Ctx("CBC")
	if err != nil {
		t.Fatalf("NewDH1080Ctx bob: %v", err)
	}
	if err := bob.Handle(alice.InitMessage()); err != nil {
		t.Fatalf("bob Handle init: %v", err)
	}
	if err := alice.Handle(bob.FinishMessage()); err != nil {
		t.Fatalf("alice Handle finish: %v", err)
	}
	aliceKey, err := alice.NegotiatedKey()
	if err != nil {
		t.Fatalf("alice NegotiatedKey: %v", err)
	}
	bobKey, err := bob.NegotiatedKey()
	if err != nil {
		t.Fatalf("bob NegotiatedKey: %v", err)
	}
	if aliceKey != bobKey {
		t.Fatalf("negotiated keys differ:\nalice=%q\nbob=%q", aliceKey, bobKey)
	}
}
