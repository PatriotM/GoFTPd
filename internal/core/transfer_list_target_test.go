package core

import "testing"

func TestListRequestTargetArgSkipsListOptions(t *testing.T) {
	target := listRequestTargetArg("LIST", []string{"-al", "/incoming/release"})
	if target != "/incoming/release" {
		t.Fatalf("expected list target after options, got %q", target)
	}
}

func TestListRequestTargetArgSkipsNLSTOptions(t *testing.T) {
	target := listRequestTargetArg("NLST", []string{"-a", "release-file"})
	if target != "release-file" {
		t.Fatalf("expected NLST target after options, got %q", target)
	}
}

func TestResolveListTargetPathUsesPreparedListTarget(t *testing.T) {
	s := &Session{
		CurrentDir: "/archive",
		PretCmd:    "LIST",
		PretArg:    "release",
	}

	target := s.resolveListTargetPath("LIST", nil, nil)
	if target != "/archive/release" {
		t.Fatalf("expected prepared list target to resolve, got %q", target)
	}
}

func TestResolveListTargetPathPrefersExplicitCommandTarget(t *testing.T) {
	s := &Session{
		CurrentDir: "/archive",
		PretCmd:    "LIST",
		PretArg:    "/old",
	}

	target := s.resolveListTargetPath("MLSD", []string{"new"}, nil)
	if target != "/archive/new" {
		t.Fatalf("expected explicit command target to win, got %q", target)
	}
}

func TestHasPreparedDataConnection(t *testing.T) {
	s := &Session{}
	if s.hasPreparedDataConnection() {
		t.Fatalf("expected empty session to have no prepared data connection")
	}

	s.ActiveAddr = "127.0.0.1:2121"
	if !s.hasPreparedDataConnection() {
		t.Fatalf("expected active address to count as prepared data connection")
	}
}

func TestHasPreparedTransferChannelAcceptsPassthrough(t *testing.T) {
	s := &Session{PassthruSlave: "SLAVE1"}
	if !s.hasPreparedTransferChannel() {
		t.Fatalf("expected passthrough slave to count as prepared transfer channel")
	}
}
