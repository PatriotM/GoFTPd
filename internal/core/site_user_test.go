package core

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestListDeletedUsersFiltersPasswdFiles(t *testing.T) {
	tmp := t.TempDir()
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	defer func() { _ = os.Chdir(oldWD) }()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("Chdir() error = %v", err)
	}
	if err := os.MkdirAll(deletedUsersDir, 0755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	for _, name := range []string{"bob", "alice", "alice.passwd"} {
		if err := os.WriteFile(filepath.Join(deletedUsersDir, name), []byte("x"), 0600); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", name, err)
		}
	}

	got, err := listDeletedUsers()
	if err != nil {
		t.Fatalf("listDeletedUsers() error = %v", err)
	}
	want := []string{"alice", "bob"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("listDeletedUsers() = %#v, want %#v", got, want)
	}
}
