package core

import (
	"net"
	"testing"
)

func TestParsePortTargetParsesValidTuple(t *testing.T) {
	ip, port, err := parsePortTarget("192,0,2,10,7,138")
	if err != nil {
		t.Fatalf("parsePortTarget returned error: %v", err)
	}
	if got := ip.String(); got != "192.0.2.10" {
		t.Fatalf("ip = %s, want 192.0.2.10", got)
	}
	if port != 1930 {
		t.Fatalf("port = %d, want 1930", port)
	}
}

func TestParsePortTargetRejectsInvalidOctet(t *testing.T) {
	if _, _, err := parsePortTarget("192,0,2,999,7,138"); err == nil {
		t.Fatalf("expected invalid octet to fail")
	}
}

func TestShouldRejectPortTargetRejectsPrivateDataForPublicControl(t *testing.T) {
	if !shouldRejectPortTarget(net.ParseIP("198.51.100.7"), net.ParseIP("192.168.1.10")) {
		t.Fatalf("expected private data target from public control IP to be rejected")
	}
	if shouldRejectPortTarget(net.ParseIP("192.168.1.20"), net.ParseIP("192.168.1.10")) {
		t.Fatalf("expected private control IP to allow private data target")
	}
}

func TestPortTargetWarningsIncludeLoopbackAndFXPHint(t *testing.T) {
	warnings := portTargetWarnings(net.ParseIP("198.51.100.7"), net.ParseIP("127.0.0.1"))
	if len(warnings) != 2 {
		t.Fatalf("expected two warnings, got %d", len(warnings))
	}
}
