package core

import (
	"net"
	"testing"
)

func TestClearPreparedTransferStateResetsPendingSetup(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen error: %v", err)
	}
	defer l.Close()

	s := &Session{
		DataListen:      l,
		ActiveAddr:      "198.51.100.10:12345",
		PassthruSlave:   "SLAVE1",
		PassthruXferIdx: 77,
		PretCmd:         "STOR",
		PretArg:         "release.r00",
		RestOffset:      1234,
	}
	s.clearPreparedTransferState()

	if s.DataListen != nil {
		t.Fatalf("expected DataListen to be cleared")
	}
	if s.ActiveAddr != "" || s.PassthruSlave != nil || s.PassthruXferIdx != 0 || s.PretCmd != "" || s.PretArg != "" || s.RestOffset != 0 {
		t.Fatalf("expected all prepared transfer state to be cleared")
	}
}

func TestClearPassiveTransferSetupKeepsPretButDropsPassiveState(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen error: %v", err)
	}
	defer l.Close()

	s := &Session{
		DataListen:      l,
		PassthruSlave:   "SLAVE1",
		PassthruXferIdx: 77,
		ActiveAddr:      "198.51.100.10:12345",
		PretCmd:         "RETR",
		PretArg:         "file.r00",
	}
	s.clearPassiveTransferSetup()

	if s.DataListen != nil || s.PassthruSlave != nil || s.PassthruXferIdx != 0 {
		t.Fatalf("expected passive setup to be cleared")
	}
	if s.ActiveAddr == "" || s.PretCmd != "RETR" || s.PretArg != "file.r00" {
		t.Fatalf("expected active target and PRET state to be preserved")
	}
}

func TestClearActiveTransferSetupKeepsPassiveState(t *testing.T) {
	s := &Session{
		DataListen:      nil,
		PassthruSlave:   "SLAVE1",
		PassthruXferIdx: 77,
		ActiveAddr:      "198.51.100.10:12345",
		PretCmd:         "STOR",
		PretArg:         "file.r00",
	}
	s.clearActiveTransferSetup()

	if s.ActiveAddr != "" {
		t.Fatalf("expected active target to be cleared")
	}
	if s.PassthruSlave == nil || s.PassthruXferIdx != 77 || s.PretCmd != "STOR" || s.PretArg != "file.r00" {
		t.Fatalf("expected passive/prepared state to be preserved")
	}
}
