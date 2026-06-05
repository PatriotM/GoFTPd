package core

import (
	"net"
	"testing"

	"goftpd/internal/user"
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

func TestCountTransfersForUserCountsByDirection(t *testing.T) {
	uploadServer, uploadClient := net.Pipe()
	defer uploadClient.Close()
	downloadServer, downloadClient := net.Pipe()
	defer downloadClient.Close()

	uploadSession := &Session{
		Conn:     uploadServer,
		IsLogged: true,
		User:     &user.User{Name: "tester"},
	}
	uploadSession.beginTransfer("upload", "/UPLOAD/release/file.r00")
	uploadSession.ID = registerSession(uploadSession)
	defer unregisterSession(uploadSession.ID)

	downloadSession := &Session{
		Conn:     downloadServer,
		IsLogged: true,
		User:     &user.User{Name: "tester"},
	}
	downloadSession.beginTransfer("download", "/UPLOAD/release/file.r01")
	downloadSession.ID = registerSession(downloadSession)
	defer unregisterSession(downloadSession.ID)

	if got := countTransfersForUser("tester", "upload"); got != 1 {
		t.Fatalf("countTransfersForUser(upload) = %d, want 1", got)
	}
	if got := countTransfersForUser("tester", "download"); got != 1 {
		t.Fatalf("countTransfersForUser(download) = %d, want 1", got)
	}
	if got := countTransfersForUser("tester", "other"); got != 0 {
		t.Fatalf("countTransfersForUser(other) = %d, want 0", got)
	}
}

func TestUploadTransferReservesPath(t *testing.T) {
	firstServer, firstClient := net.Pipe()
	defer firstClient.Close()
	secondServer, secondClient := net.Pipe()
	defer secondClient.Close()

	first := &Session{
		Conn:     firstServer,
		IsLogged: true,
		User:     &user.User{Name: "first"},
	}
	first.ID = registerSession(first)
	defer unregisterSession(first.ID)

	second := &Session{
		Conn:     secondServer,
		IsLogged: true,
		User:     &user.User{Name: "second"},
	}
	second.ID = registerSession(second)
	defer unregisterSession(second.ID)

	if !first.tryBeginUploadTransfer("/X265/Release-GRP/file.r00") {
		t.Fatalf("expected first upload reservation to succeed")
	}
	if !activeUploadForPath("/X265/Release-GRP/file.r00") {
		t.Fatalf("expected active upload lookup to see reserved path")
	}
	if second.tryBeginUploadTransfer("/X265/Release-GRP/file.r00") {
		t.Fatalf("expected second upload reservation for same path to fail")
	}

	first.endTransfer()

	if !second.tryBeginUploadTransfer("/X265/Release-GRP/file.r00") {
		t.Fatalf("expected reservation to be reusable after first upload ends")
	}
}
