package core

import "testing"

func TestParseSiteRemergeArgsDefaultsToAllRoots(t *testing.T) {
	req, err := parseSiteRemergeArgs([]string{"LOCAL"})
	if err != nil {
		t.Fatalf("parseSiteRemergeArgs returned error: %v", err)
	}
	if req.target != "LOCAL" {
		t.Fatalf("target = %q, want LOCAL", req.target)
	}
	if req.jobName != "" || req.path != "" {
		t.Fatalf("unexpected scoped args: %+v", req)
	}
}

func TestParseSiteRemergeArgsAllowsWildcardTarget(t *testing.T) {
	req, err := parseSiteRemergeArgs([]string{"*"})
	if err != nil {
		t.Fatalf("parseSiteRemergeArgs returned error: %v", err)
	}
	if req.target != "*" {
		t.Fatalf("target = %q, want *", req.target)
	}
}

func TestParseSiteRemergeArgsAllowsJobAndPath(t *testing.T) {
	req, err := parseSiteRemergeArgs([]string{"LOCAL", "mounted_roots", "/ARCHiVE/BLURAY"})
	if err != nil {
		t.Fatalf("parseSiteRemergeArgs returned error: %v", err)
	}
	if req.target != "LOCAL" || req.jobName != "mounted_roots" || req.path != "/ARCHiVE/BLURAY" {
		t.Fatalf("request = %+v, want LOCAL mounted_roots /ARCHiVE/BLURAY", req)
	}
}

func TestParseSiteRemergeArgsRejectsRelativePath(t *testing.T) {
	if _, err := parseSiteRemergeArgs([]string{"LOCAL", "mounted_roots", "ARCHiVE/BLURAY"}); err == nil {
		t.Fatalf("expected relative path to be rejected")
	}
}

func TestParseSiteRemergeArgsRejectsTooManyArgs(t *testing.T) {
	if _, err := parseSiteRemergeArgs([]string{"LOCAL", "mounted_roots", "/ARCHiVE", "extra"}); err == nil {
		t.Fatalf("expected too many args to be rejected")
	}
}
