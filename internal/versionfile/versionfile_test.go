package versionfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadPrefersSiblingVersionFile(t *testing.T) {
	root := t.TempDir()
	cfgPath := filepath.Join(root, "etc", "config.yml")
	if err := os.MkdirAll(filepath.Dir(cfgPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "etc", "version"), []byte("1.2.3\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got := Load(cfgPath, "fallback")
	if got != "1.2.3" {
		t.Fatalf("Load() = %q, want %q", got, "1.2.3")
	}
}

func TestLoadFallsBackToAncestorEtcVersion(t *testing.T) {
	root := t.TempDir()
	cfgPath := filepath.Join(root, "sitebot", "etc", "config.yml")
	if err := os.MkdirAll(filepath.Dir(cfgPath), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(root, "etc"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "etc", "version"), []byte("2.0.0\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got := Load(cfgPath, "fallback")
	if got != "2.0.0" {
		t.Fatalf("Load() = %q, want %q", got, "2.0.0")
	}
}
