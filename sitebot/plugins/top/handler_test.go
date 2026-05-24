package top

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

func TestBuildLinesShowsActualUploaderCount(t *testing.T) {
	tmp := t.TempDir()
	usersDir := filepath.Join(tmp, "users")
	if err := os.MkdirAll(usersDir, 0755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	now := time.Now()
	for _, name := range []string{"alpha", "beta", "gamma"} {
		path := filepath.Join(usersDir, name)
		content := "DAYUP 1 1048576 0\nTIME 0 0 0 0 " + strconv.FormatInt(now.Unix(), 10) + "\nPRIMARY_GROUP iND\n"
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", name, err)
		}
	}
	p := New()
	p.usersDir = usersDir
	lines, err := p.buildLines(2, true)
	if err != nil {
		t.Fatalf("buildLines() error = %v", err)
	}
	if len(lines) == 0 || !strings.Contains(lines[0], "3 Users") {
		t.Fatalf("header line = %q, want total uploader count", strings.Join(lines, "\n"))
	}
}

func TestBuildLinesExcludesConfiguredUsersAndGroups(t *testing.T) {
	tmp := t.TempDir()
	usersDir := filepath.Join(tmp, "users")
	if err := os.MkdirAll(usersDir, 0755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	now := time.Now()
	entries := []struct {
		user  string
		group string
		files int
		bytes int
	}{
		{user: "alpha", group: "iND", files: 5, bytes: 5000},
		{user: "botuser", group: "iND", files: 10, bytes: 10000},
		{user: "gamma", group: "SiteOP", files: 20, bytes: 20000},
	}
	for _, entry := range entries {
		path := filepath.Join(usersDir, entry.user)
		content := "DAYUP " + strconv.Itoa(entry.files) + " " + strconv.Itoa(entry.bytes) + " 0\nTIME 0 0 0 0 " + strconv.FormatInt(now.Unix(), 10) + "\nPRIMARY_GROUP " + entry.group + "\n"
		if err := os.WriteFile(path, []byte(content), 0600); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", entry.user, err)
		}
	}

	p := New()
	p.usersDir = usersDir
	p.excludedUsers = lowerStringSet([]string{"BOTUSER"})
	p.excludedGroups = lowerStringSet([]string{"siteop"})

	lines, err := p.buildLines(10, true)
	if err != nil {
		t.Fatalf("buildLines() error = %v", err)
	}
	output := strings.Join(lines, "\n")
	if strings.Contains(output, "botuser") || strings.Contains(output, "gamma") {
		t.Fatalf("excluded users/groups still shown:\n%s", output)
	}
	if !strings.Contains(output, "alpha") || !strings.Contains(output, "1 Users") {
		t.Fatalf("expected only alpha in output:\n%s", output)
	}
	if !strings.Contains(output, "5 Files") {
		t.Fatalf("expected totals to exclude filtered users:\n%s", output)
	}
}
