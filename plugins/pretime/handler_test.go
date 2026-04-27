package pretime

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"goftpd/internal/plugin"
)

func TestLookupSQLiteCustomFields(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "releases.db")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE pretimes (rls TEXT PRIMARY KEY, unixts INTEGER)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO pretimes (rls, unixts) VALUES (?, ?)`, "Test.Release-GRP", 1714200000); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	ts, err := lookupSQL("sqlite3", dbPath, "pretimes", "rls", "unixts", "Test.Release-GRP")
	if err != nil {
		t.Fatalf("lookupSQL: %v", err)
	}
	if ts != 1714200000 {
		t.Fatalf("expected 1714200000, got %d", ts)
	}
}

func TestProcessJobEmitsOldPreTime(t *testing.T) {
	h := New()
	h.debug = true
	h.sqlite.Enabled = true
	h.sqlite.Path = filepath.Join(t.TempDir(), "releases.db")
	h.sqlite.Table = "releases"
	h.sqlite.ReleaseField = "release"
	h.sqlite.UnixTimeField = "timestamp_unix"
	db, err := sql.Open("sqlite3", h.sqlite.Path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE releases (release TEXT PRIMARY KEY, timestamp_unix INTEGER)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	oldUnix := time.Now().Add(-2 * time.Hour).Unix()
	if _, err := db.Exec(`INSERT INTO releases (release, timestamp_unix) VALUES (?, ?)`, "Old.Release-GRP", oldUnix); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	_ = db.Close()

	type emitted struct {
		evtType string
		path    string
		file    string
		section string
		data    map[string]string
	}
	var got emitted
	h.svc = &plugin.Services{
		EmitEvent: func(eventType, p, filename, section string, size int64, speed float64, data map[string]string) {
			got = emitted{evtType: eventType, path: p, file: filename, section: section, data: data}
		},
	}
	h.processJob(job{
		path:    "/0DAY/2026-04-27/Old.Release-GRP",
		relname: "Old.Release-GRP",
		section: "0DAY",
		user:    "N0pe",
		group:   "Admin",
	})

	if got.evtType != eventOldPreTime {
		t.Fatalf("expected %s, got %s", eventOldPreTime, got.evtType)
	}
	if got.data["provider"] != "sqlite" {
		t.Fatalf("expected sqlite provider, got %q", got.data["provider"])
	}
	if got.data["relname"] != "Old.Release-GRP" {
		t.Fatalf("unexpected relname %q", got.data["relname"])
	}
	if got.data["preage"] == "" {
		t.Fatalf("expected preage to be set")
	}
}

func TestIsReleaseDirSupportsDatedLayout(t *testing.T) {
	if !isReleaseDir("/0DAY/2026-04-27/Test.Release-GRP", "0DAY") {
		t.Fatalf("expected dated release dir to match")
	}
	if isReleaseDir("/0DAY/2026-04-27/Sample", "0DAY") {
		t.Fatalf("did not expect helper subdir to match as release root")
	}
}
