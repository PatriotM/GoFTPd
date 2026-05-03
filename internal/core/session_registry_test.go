package core

import (
	"net"
	"testing"
)

func TestDisconnectActiveSessionClearsTransferState(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()

	s := &Session{
		Conn:       server,
		IsLogged:   true,
		CurrentDir: "/",
	}
	s.beginTransferOnSlave("download", "/MP3/Test.Release-GRP/file.mp3", "SLAVE1", 42)
	s.ID = registerSession(s)
	defer unregisterSession(s.ID)

	if !DisconnectActiveSession(s.ID) {
		t.Fatalf("expected disconnect to succeed")
	}

	snaps := listActiveSessions()
	if len(snaps) != 1 {
		t.Fatalf("expected one active session snapshot, got %d", len(snaps))
	}
	snap := snaps[0]
	if snap.TransferDirection != "" {
		t.Fatalf("expected transfer direction to be cleared, got %q", snap.TransferDirection)
	}
	if snap.TransferPath != "" {
		t.Fatalf("expected transfer path to be cleared, got %q", snap.TransferPath)
	}
	if snap.TransferSlaveName != "" || snap.TransferSlaveIdx != 0 {
		t.Fatalf("expected slave transfer identity to be cleared, got %q/%d", snap.TransferSlaveName, snap.TransferSlaveIdx)
	}
}
