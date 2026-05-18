package autonuke

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"goftpd/internal/plugin"
)

type testBridge struct {
	entries map[string][]plugin.FileEntry
}

func (b *testBridge) PluginListDir(path string) []plugin.FileEntry {
	return append([]plugin.FileEntry(nil), b.entries[path]...)
}
func (b *testBridge) MakeDir(path, owner, group string) error   { return nil }
func (b *testBridge) Symlink(linkPath, targetPath string) error { return nil }
func (b *testBridge) VFSSymlink(linkPath, targetPath string) error {
	return nil
}
func (b *testBridge) Chmod(path string, mode uint32) error { return nil }
func (b *testBridge) CreateSparseFile(path string, size int64, owner, group string) error {
	return nil
}
func (b *testBridge) DeleteFile(path string) error                  { return nil }
func (b *testBridge) RenameFile(from, toDir, toName string) error   { return nil }
func (b *testBridge) RelocatePath(from, toDir, toName string) error { return nil }
func (b *testBridge) RelocatePathToSlave(from, toDir, toName, targetSlave string) error {
	return nil
}
func (b *testBridge) WriteFile(path string, content []byte) error { return nil }
func (b *testBridge) ReadFile(path string) ([]byte, error)        { return nil, nil }
func (b *testBridge) ProbeMediaInfo(path, binary string, timeoutSeconds int) (map[string]string, error) {
	return nil, nil
}
func (b *testBridge) CacheMediaInfo(path string, fields map[string]string) {}
func (b *testBridge) FileExists(path string) bool                          { return false }
func (b *testBridge) GetFileSize(path string) int64                        { return -1 }
func (b *testBridge) GetSFVData(dirPath string) map[string]uint32          { return nil }
func (b *testBridge) GetRequestData(dirPath string) ([]plugin.RequestRecord, []plugin.RequestFillRecord) {
	return nil, nil
}
func (b *testBridge) SetRequestData(dirPath string, requests []plugin.RequestRecord, fills []plugin.RequestFillRecord) {
}
func (b *testBridge) GetDirMediaInfo(dirPath string) map[string]string { return nil }
func (b *testBridge) PluginGetVFSRaceStats(dirPath string) (users []plugin.RaceUser, groups []plugin.RaceGroup, totalBytes int64, present int, total int) {
	return nil, nil, 0, 0, 0
}

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

func TestExpandPatternSkipsSymlinkCandidates(t *testing.T) {
	h := &Handler{
		svc: &plugin.Services{
			Bridge: &testBridge{
				entries: map[string][]plugin.FileEntry{
					"/MP3": {
						{Name: "!Today_MP3", IsDir: true, IsSymlink: true},
						{Name: "0512", IsDir: true},
					},
					"/MP3/0512": {
						{Name: "Real.Release-2026-GRP", IsDir: true},
						{Name: "!Linked.Release", IsDir: true, IsSymlink: true},
					},
				},
			},
		},
	}

	got := h.expandPattern("/MP3/*")
	if len(got) != 1 {
		t.Fatalf("expected only one real release candidate, got %+v", got)
	}
	if got[0].Path != "/MP3/0512/Real.Release-2026-GRP" {
		t.Fatalf("unexpected release candidate: %+v", got[0])
	}
}

func TestTimedNukeReason(t *testing.T) {
	tests := []struct {
		name    string
		base    string
		minutes int
		want    string
	}{
		{name: "empty with minutes", base: "Empty", minutes: 240, want: "Empty after 240 minutes"},
		{name: "singular minute", base: "Incomplete", minutes: 1, want: "Incomplete after 1 minute"},
		{name: "no minutes", base: "Half-Empty", minutes: 0, want: "Half-Empty"},
		{name: "blank base", base: "", minutes: 60, want: "Autonuke after 60 minutes"},
	}
	for _, tt := range tests {
		if got := timedNukeReason(tt.base, tt.minutes); got != tt.want {
			t.Fatalf("%s: timedNukeReason(%q, %d) = %q, want %q", tt.name, tt.base, tt.minutes, got, tt.want)
		}
	}
}
