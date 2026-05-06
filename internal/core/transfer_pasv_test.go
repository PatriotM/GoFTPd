package core

import "testing"

func TestFtpPassiveIPv4FormatsValidAddress(t *testing.T) {
	got, err := ftpPassiveIPv4("203.0.113.10")
	if err != nil {
		t.Fatalf("ftpPassiveIPv4 returned error: %v", err)
	}
	if got != "203,0,113,10" {
		t.Fatalf("ftpPassiveIPv4 = %q, want %q", got, "203,0,113,10")
	}
}

func TestFtpPassiveIPv4RejectsInvalidAddress(t *testing.T) {
	if _, err := ftpPassiveIPv4(""); err == nil {
		t.Fatalf("expected empty passive IP to fail")
	}
	if _, err := ftpPassiveIPv4("not-an-ip"); err == nil {
		t.Fatalf("expected invalid passive IP to fail")
	}
}
