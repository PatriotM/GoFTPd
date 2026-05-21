package core

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAuthStateBackupOnceCreatesDaemonBackups(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)

	mustMkdir(t, filepath.Join("etc", "users"))
	mustMkdir(t, filepath.Join("etc", "groups"))
	mustWrite(t, filepath.Join("etc", "passwd"), "Finity:hash:100:300:0:/site:/bin/false\n", 0600)
	mustWrite(t, filepath.Join("etc", "group"), "iND::300:\n", 0644)
	mustWrite(t, filepath.Join("etc", "permissions.yml"), "rules: []\n", 0644)
	mustWrite(t, filepath.Join("etc", "affils.yml"), "affils: []\n", 0644)
	mustWrite(t, filepath.Join("etc", "users", "Finity"), "USER test\nGROUP iND 0\n", 0600)
	mustWrite(t, filepath.Join("etc", "groups", "iND"), "GROUP iND\n", 0644)

	cfg := &Config{Mode: "master", PasswdFile: filepath.Join("etc", "passwd")}
	if err := authStateBackupOnce(cfg); err != nil {
		t.Fatalf("authStateBackupOnce() error = %v", err)
	}

	assertFile(t, filepath.Join("etc", "passwd-"), "Finity:hash:100:300:0:/site:/bin/false\n")
	assertFile(t, filepath.Join("etc", "group-"), "iND::300:\n")
	assertFile(t, filepath.Join("etc", "permissions.yml-"), "rules: []\n")
	assertFile(t, filepath.Join("etc", "affils.yml-"), "affils: []\n")
	assertFile(t, filepath.Join("etc", "users-", "Finity"), "USER test\nGROUP iND 0\n")
	assertFile(t, filepath.Join("etc", "groups-", "iND"), "GROUP iND\n")
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatalf("MkdirAll(%s) error = %v", path, err)
	}
}

func mustWrite(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatalf("WriteFile(%s) error = %v", path, err)
	}
}

func assertFile(t *testing.T, path, want string) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s) error = %v", path, err)
	}
	if string(got) != want {
		t.Fatalf("%s = %q, want %q", path, string(got), want)
	}
}
