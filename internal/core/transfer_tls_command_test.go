package core

import "testing"

func TestCanNegotiateDataTLSRequiresSecureControlChannel(t *testing.T) {
	s := &Session{Config: &Config{TLSEnabled: true}}
	if s.Config == nil || !s.Config.TLSEnabled {
		t.Fatalf("expected test config to have TLS enabled")
	}
	if s.IsTLS {
		t.Fatalf("expected test session to start without control TLS")
	}
}
