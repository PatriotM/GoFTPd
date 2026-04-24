package zipscript

import "testing"

func TestUsesReleaseCheckEntryRequiresExactReleaseDepth(t *testing.T) {
	cfg := Config{
		Enabled: true,
		Sections: SectionsConfig{
			ReleaseCheck: []string{"/APPS/*", "/MP3/*/*"},
		},
	}

	if UsesReleaseCheckEntry(cfg, "/APPS") {
		t.Fatalf("section root should not count as a release entry")
	}
	if !UsesReleaseCheckEntry(cfg, "/APPS/Some.Release-GRP") {
		t.Fatalf("direct release path should match")
	}
	if UsesReleaseCheckEntry(cfg, "/APPS/Some.Release-GRP/CD1") {
		t.Fatalf("descendant path should not count as the release entry itself")
	}
	if UsesReleaseCheckEntry(cfg, "/MP3/0424") {
		t.Fatalf("dated container should not count as a release entry")
	}
	if !UsesReleaseCheckEntry(cfg, "/MP3/0424/Artist-Album-2026-GRP") {
		t.Fatalf("dated release path should match")
	}
}
