package core

import "testing"

func TestParseSiteRemergeArgsDefaultsToAllRoots(t *testing.T) {
	target, err := parseSiteRemergeArgs([]string{"LOCAL"})
	if err != nil {
		t.Fatalf("parseSiteRemergeArgs returned error: %v", err)
	}
	if target != "LOCAL" {
		t.Fatalf("target = %q, want LOCAL", target)
	}
}

func TestParseSiteRemergeArgsAllowsWildcardTarget(t *testing.T) {
	target, err := parseSiteRemergeArgs([]string{"*"})
	if err != nil {
		t.Fatalf("parseSiteRemergeArgs returned error: %v", err)
	}
	if target != "*" {
		t.Fatalf("target = %q, want *", target)
	}
}

func TestParseSiteRemergeArgsRejectsExtraArgs(t *testing.T) {
	if _, err := parseSiteRemergeArgs([]string{"LOCAL", "anything"}); err == nil {
		t.Fatalf("expected extra args to be rejected")
	}
}
