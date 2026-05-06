package core

import "testing"

func TestValidatePretRequestRejectsUnsupportedCommand(t *testing.T) {
	if _, _, err := validatePretRequest(&Session{}, "RNFR", []string{"file"}); err == nil {
		t.Fatalf("expected unsupported PRET command to fail")
	}
}

func TestValidatePretRequestRequiresTargetForStorAndRetr(t *testing.T) {
	if _, _, err := validatePretRequest(&Session{}, "STOR", nil); err == nil {
		t.Fatalf("expected PRET STOR without target to fail")
	}
	if _, _, err := validatePretRequest(&Session{}, "RETR", nil); err == nil {
		t.Fatalf("expected PRET RETR without target to fail")
	}
}

func TestValidatePretRequestAllowsListFamily(t *testing.T) {
	cmd, arg, err := validatePretRequest(&Session{}, "LIST", []string{"/X265"})
	if err != nil {
		t.Fatalf("validatePretRequest returned error: %v", err)
	}
	if cmd != "LIST" || arg != "/X265" {
		t.Fatalf("unexpected prepared values: %q %q", cmd, arg)
	}
}

func TestPretSuccessMessage(t *testing.T) {
	if got := pretSuccessMessage("LIST"); got != "OK, planning to use master for upcoming LIST transfer" {
		t.Fatalf("unexpected LIST PRET message: %q", got)
	}
	if got := pretSuccessMessage("RETR"); got != "OK, planning for upcoming download" {
		t.Fatalf("unexpected RETR PRET message: %q", got)
	}
	if got := pretSuccessMessage("STOR"); got != "OK, planning for upcoming upload" {
		t.Fatalf("unexpected STOR PRET message: %q", got)
	}
}
