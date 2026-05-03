package autonuke

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigNormalizesSections(t *testing.T) {
	cfg := loadConfig(map[string]interface{}{
		"sections": []interface{}{
			"/GAMES",
			"/FOREIGN/TV-FR/*",
			"/MP3/MMDD",
		},
	})

	if len(cfg.Sections) != 3 {
		t.Fatalf("expected 3 sections, got %d", len(cfg.Sections))
	}
	if cfg.Sections[0] != "/GAMES/*" {
		t.Fatalf("unexpected first section: %q", cfg.Sections[0])
	}
	if cfg.Sections[1] != "/FOREIGN/TV-FR/*" {
		t.Fatalf("unexpected second section: %q", cfg.Sections[1])
	}
	if cfg.Sections[2] != "/MP3/MMDD/*" {
		t.Fatalf("unexpected third section: %q", cfg.Sections[2])
	}
}

func TestIsProtectedBaseMatchesSectionBases(t *testing.T) {
	h := &Handler{
		cfg: config{
			Sections: []string{"/GAMES/*", "/FOREIGN/TV-FR/*"},
		},
	}

	if !h.isProtectedBase("/GAMES") {
		t.Fatalf("expected /GAMES to be treated as a protected base")
	}
	if !h.isProtectedBase("/FOREIGN/TV-FR") {
		t.Fatalf("expected /FOREIGN/TV-FR to be treated as a protected base")
	}
	if h.isProtectedBase("/FOREIGN/TV-FR/Some.Release-GRP") {
		t.Fatalf("release dir should not be treated as the protected base itself")
	}
}

func TestIsDatedBucketName(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{name: "0502", want: true},
		{name: "20260502", want: true},
		{name: "2026-05-02", want: true},
		{name: "18-2026", want: true},
		{name: "2026-18", want: true},
		{name: "Some.Release-GRP", want: false},
		{name: "TV-DE", want: false},
	}
	for _, tc := range cases {
		if got := isDatedBucketName(tc.name); got != tc.want {
			t.Fatalf("isDatedBucketName(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestIsProtectedBaseWithDatedSectionPattern(t *testing.T) {
	h := &Handler{
		cfg: config{
			Sections: []string{
				"/TV-1080P/*",
				"/FOREIGN/TV-FR/*",
				"/XXX-0DAY/MMDD/*",
			},
		},
	}
	today := time.Now().Format("0102")

	cases := []struct {
		path string
		want bool
	}{
		{path: "/TV-1080P", want: true},
		{path: "/FOREIGN/TV-FR", want: true},
		{path: "/XXX-0DAY", want: true},
		{path: "/TV-1080P/Some.Release-GRP", want: false},
		{path: "/XXX-0DAY/" + today, want: false},
	}

	for _, tc := range cases {
		if got := h.isProtectedBase(tc.path); got != tc.want {
			t.Fatalf("isProtectedBase(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestAppendHistoryWritesJSONL(t *testing.T) {
	tmp := t.TempDir()
	h := &Handler{cfg: config{StateDir: tmp}}
	rel := releaseCandidate{
		Path:    "/TV-1080P/Example.Release-GRP",
		Name:    "Example.Release-GRP",
		Section: "TV-1080P",
		Owner:   "N0pe",
	}

	h.appendHistory("nuked", rel, "Empty", "multiplier=x10")

	data, err := os.ReadFile(filepath.Join(tmp, "history.jsonl"))
	if err != nil {
		t.Fatalf("read history.jsonl: %v", err)
	}
	text := string(data)
	for _, want := range []string{`"action":"nuked"`, `"path":"/TV-1080P/Example.Release-GRP"`, `"reason":"Empty"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("history output missing %s: %s", want, text)
		}
	}
}
