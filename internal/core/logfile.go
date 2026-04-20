package core

import (
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// rotatingLog is an io.Writer that writes to a file and rotates it daily.
// The active file is always `<basePath>` (e.g. logs/goftpd.log). At midnight
// (local time), the previous day's file is renamed to `<basePath>.YYYY-MM-DD`
// and any rotated file older than `keepDays` is deleted.
type rotatingLog struct {
	mu       sync.Mutex
	basePath string
	keepDays int
	f        *os.File
	day      string // YYYY-MM-DD of the currently-open file
}

func newRotatingLog(basePath string, keepDays int) (*rotatingLog, error) {
	if keepDays < 1 {
		keepDays = 1
	}
	if err := os.MkdirAll(filepath.Dir(basePath), 0755); err != nil {
		return nil, err
	}
	r := &rotatingLog{basePath: basePath, keepDays: keepDays}
	if err := r.open(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *rotatingLog) open() error {
	f, err := os.OpenFile(r.basePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	r.f = f
	r.day = time.Now().Format("2006-01-02")
	return nil
}

func (r *rotatingLog) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	today := time.Now().Format("2006-01-02")
	if today != r.day {
		_ = r.rotate(today)
	}
	return r.f.Write(p)
}

// rotate closes the current file, renames it to basePath.<prev-day>, opens a
// fresh basePath, and removes rotated files older than keepDays. Called with
// the mutex already held.
func (r *rotatingLog) rotate(newDay string) error {
	if r.f != nil {
		_ = r.f.Close()
	}
	archived := r.basePath + "." + r.day
	_ = os.Rename(r.basePath, archived)

	// Open fresh active file.
	if err := r.open(); err != nil {
		return err
	}
	r.day = newDay

	// Purge old rotated files.
	dir := filepath.Dir(r.basePath)
	base := filepath.Base(r.basePath) + "."
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	cutoff := time.Now().AddDate(0, 0, -r.keepDays)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) <= len(base) || name[:len(base)] != base {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(dir, name))
		}
	}
	return nil
}

// InstallFileLogger redirects the standard `log` package to tee output to
// both stderr and a rotating file. Called from main.go right after config
// load. Only active when logFilePath != "". keepDays controls how many
// rotated files are retained (minimum 1 = today only).
func InstallFileLogger(logFilePath string, keepDays int) error {
	if logFilePath == "" {
		return nil
	}
	r, err := newRotatingLog(logFilePath, keepDays)
	if err != nil {
		return err
	}
	log.SetOutput(io.MultiWriter(os.Stderr, r))
	log.Printf("[LOG] File logging enabled: %s (keep %d days)", logFilePath, keepDays)
	return nil
}
