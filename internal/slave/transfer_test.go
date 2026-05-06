package slave

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCleanupFailedReceiveRemovesFreshUpload(t *testing.T) {
	dir := t.TempDir()
	fullPath := filepath.Join(dir, "fresh.bin")
	if err := os.WriteFile(fullPath, []byte("broken"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	cleanupFailedReceive(nil, fullPath, 0)

	if _, err := os.Stat(fullPath); !os.IsNotExist(err) {
		t.Fatalf("expected fresh failed upload to be removed, stat err=%v", err)
	}
}

func TestCleanupFailedReceiveTruncatesBackToResumeOffset(t *testing.T) {
	dir := t.TempDir()
	fullPath := filepath.Join(dir, "resume.bin")
	if err := os.WriteFile(fullPath, []byte("abcdefghij"), 0644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	file, err := os.OpenFile(fullPath, os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open file: %v", err)
	}
	defer file.Close()

	if _, err := file.Seek(6, 0); err != nil {
		t.Fatalf("seek file: %v", err)
	}
	if _, err := file.Write([]byte("WXYZ")); err != nil {
		t.Fatalf("append bytes: %v", err)
	}

	cleanupFailedReceive(file, fullPath, 6)

	data, err := os.ReadFile(fullPath)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(data) != "abcdef" {
		t.Fatalf("expected file truncated back to resume offset, got %q", string(data))
	}
}
