package core

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyPasswordSupportsLegacyGlftpdHash(t *testing.T) {
	hash := "$01020304$d631702386055e6797948aa58b4551b2ba70492a"
	if !VerifyPassword("secret", hash) {
		t.Fatalf("VerifyPassword() = false, want true for valid legacy glFTPD hash")
	}
	if VerifyPassword("wrong", hash) {
		t.Fatalf("VerifyPassword() = true, want false for wrong password")
	}
}

func TestUpgradeLegacyPasswordHashRewritesToBcrypt(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	passwdPath := filepath.Join(dir, "passwd")
	legacy := "$01020304$d631702386055e6797948aa58b4551b2ba70492a"
	if err := os.WriteFile(passwdPath, []byte("Finity:"+legacy+":100:300:0:/site:/bin/false\n"), 0600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	upgraded, err := UpgradeLegacyPasswordHash("Finity", "secret", legacy, passwdPath)
	if err != nil {
		t.Fatalf("UpgradeLegacyPasswordHash() error = %v", err)
	}
	if !upgraded {
		t.Fatalf("UpgradeLegacyPasswordHash() upgraded = false, want true")
	}

	passwds, err := LoadPasswdFile(passwdPath)
	if err != nil {
		t.Fatalf("LoadPasswdFile() error = %v", err)
	}
	got := passwds["Finity"]
	if !strings.HasPrefix(got, "$2") {
		t.Fatalf("upgraded hash = %q, want bcrypt", got)
	}
	if !VerifyPassword("secret", got) {
		t.Fatalf("VerifyPassword() = false, want true for upgraded bcrypt hash")
	}
}

func TestAddUserToPasswdPreservesExistingFields(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	passwdPath := filepath.Join(dir, "passwd")
	if err := os.WriteFile(passwdPath, []byte("Finity:oldhash:123:456:glftpd:/glroot:/bin/bash\n"), 0600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	if err := AddUserToPasswd("Finity", "newhash", passwdPath); err != nil {
		t.Fatalf("AddUserToPasswd() error = %v", err)
	}

	data, err := os.ReadFile(passwdPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	got := strings.TrimSpace(string(data))
	want := "Finity:newhash:123:456:glftpd:/glroot:/bin/bash"
	if got != want {
		t.Fatalf("passwd line = %q, want %q", got, want)
	}
}

func TestAddUserToPasswdUsesStableDefaultsForNewUsers(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	passwdPath := filepath.Join(dir, "passwd")

	if err := AddUserToPasswd("Finity", "newhash", passwdPath); err != nil {
		t.Fatalf("AddUserToPasswd() error = %v", err)
	}

	data, err := os.ReadFile(passwdPath)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	got := strings.TrimSpace(string(data))
	want := "Finity:newhash:100:300:0:/site:/bin/false"
	if got != want {
		t.Fatalf("passwd line = %q, want %q", got, want)
	}
}
