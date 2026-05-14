package top

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseCurrentDayUpFallsBackToFileModTime(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "Tester")
	content := "DAYUP 12 3456 0 0 0 0\nTIME 0 1715600000 0 0\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	now := time.Date(2026, 5, 14, 9, 45, 0, 0, time.Local)
	modTime := now.Add(-10 * time.Minute)
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}

	files, bytes, err := parseCurrentDayUp(path, now)
	if err != nil {
		t.Fatalf("parseCurrentDayUp() error = %v", err)
	}
	if files != 12 || bytes != 3456 {
		t.Fatalf("parseCurrentDayUp() = (%d, %d), want (12, 3456)", files, bytes)
	}
}
