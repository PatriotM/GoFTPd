package core

import "testing"

type fakeTransferAborter struct {
	slaveName string
	idx       int32
	reason    string
	called    bool
}

func (f *fakeTransferAborter) AbortTransfer(slaveName string, transferIndex int32, reason string) bool {
	f.called = true
	f.slaveName = slaveName
	f.idx = transferIndex
	f.reason = reason
	return true
}

func TestAbortCurrentTransferUsesBridgeAbort(t *testing.T) {
	bridge := &fakeTransferAborter{}
	s := &Session{MasterManager: bridge}
	s.beginTransferOnSlave("download", "/MP3/release/file.r00", "SLAVE1", 42)
	s.PretCmd = "RETR"
	s.PretArg = "file.r00"
	s.PassthruSlave = "SLAVE1"
	s.PassthruXferIdx = 42

	if !s.abortCurrentTransfer("manual abort") {
		t.Fatalf("expected abortCurrentTransfer to report an aborted transfer")
	}
	if !bridge.called {
		t.Fatalf("expected bridge AbortTransfer to be called")
	}
	if bridge.slaveName != "SLAVE1" || bridge.idx != 42 || bridge.reason != "manual abort" {
		t.Fatalf("unexpected abort args: %#v", bridge)
	}
	if s.TransferDirection != "" || s.TransferPath != "" || s.TransferSlaveName != "" || s.TransferSlaveIdx != 0 {
		t.Fatalf("expected transfer state to be cleared after abort")
	}
	if s.PretCmd != "" || s.PretArg != "" || s.PassthruSlave != nil || s.PassthruXferIdx != 0 {
		t.Fatalf("expected pending passthrough state to be cleared after abort")
	}
}
