package core

import "testing"

func TestCanUseSecureFXPRequiresControlAndDataTLS(t *testing.T) {
	s := &Session{}
	if canUseSecureFXP(s) {
		t.Fatalf("expected secure FXP to be disabled without TLS")
	}
	s.IsTLS = true
	if canUseSecureFXP(s) {
		t.Fatalf("expected secure FXP to require data TLS too")
	}
	s.DataTLS = true
	if !canUseSecureFXP(s) {
		t.Fatalf("expected secure FXP when both control and data TLS are enabled")
	}
}
