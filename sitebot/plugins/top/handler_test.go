package top

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseDayUploadSnapshotFallsBackToFileModTime(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "Tester")
	content := "DAYUP 12 3456 0 0 0 0\nTIME 0 1715600000 0 0\nPRIMARY_GROUP iND\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	now := time.Date(2026, 5, 14, 9, 45, 0, 0, time.Local)
	modTime := now.Add(-10 * time.Minute)
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}

	stat, err := parseDayUploadSnapshot(path, "Tester", now)
	if err != nil {
		t.Fatalf("parseDayUploadSnapshot() error = %v", err)
	}
	if stat.User != "Tester" || stat.Group != "iND" || stat.Files != 12 || stat.Bytes != 3456 {
		t.Fatalf("parseDayUploadSnapshot() = %+v", stat)
	}
}
