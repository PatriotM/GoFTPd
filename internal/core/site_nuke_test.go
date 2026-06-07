package core

import (
	"bytes"
	"strings"
	"testing"
)

func TestResolveSiteDirAbsoluteMissingDoesNotSearchArchiveCopy(t *testing.T) {
	var out bytes.Buffer
	searched := false

	got, ok := resolveSiteDirPath(&out, "/", "/MP3/0605/Release-GRP", false,
		func(string) bool { return false },
		func(string, int) []VFSSearchResult {
			searched = true
			return []VFSSearchResult{{Path: "/ARCHiVE/MP3/0605/Release-GRP"}}
		},
	)

	if ok || got != "" {
		t.Fatalf("resolveSiteDirPath returned %q, %v; want missing absolute path to fail", got, ok)
	}
	if searched {
		t.Fatal("resolveSiteDirPath searched globally for a missing absolute path")
	}
	if !strings.Contains(out.String(), "550 Directory not found: /MP3/0605/Release-GRP") {
		t.Fatalf("unexpected response: %q", out.String())
	}
}

func TestResolveSiteDirExistingWrongNukeStateDoesNotSearch(t *testing.T) {
	var out bytes.Buffer
	searched := false

	got, ok := resolveSiteDirPath(&out, "/", "/MP3/[NUKED]-Release-GRP", false,
		func(p string) bool { return p == "/MP3/[NUKED]-Release-GRP" },
		func(string, int) []VFSSearchResult {
			searched = true
			return []VFSSearchResult{{Path: "/MP3/Release-GRP"}}
		},
	)

	if ok || got != "" {
		t.Fatalf("resolveSiteDirPath returned %q, %v; want wrong nuke state to fail", got, ok)
	}
	if searched {
		t.Fatal("resolveSiteDirPath searched globally after exact path had wrong nuke state")
	}
	if !strings.Contains(out.String(), "550 Directory is already nuked: /MP3/[NUKED]-Release-GRP") {
		t.Fatalf("unexpected response: %q", out.String())
	}
}

func TestResolveSiteDirRelativeTargetKeepsSearchFallback(t *testing.T) {
	var out bytes.Buffer
	searched := false

	got, ok := resolveSiteDirPath(&out, "/MP3/0605", "Release-GRP", false,
		func(string) bool { return false },
		func(query string, limit int) []VFSSearchResult {
			searched = true
			if query != "Release-GRP" || limit != siteSearchLimit {
				t.Fatalf("unexpected search query=%q limit=%d", query, limit)
			}
			return []VFSSearchResult{{Path: "/MP3/0605/Release-GRP"}}
		},
	)

	if !ok || got != "/MP3/0605/Release-GRP" {
		t.Fatalf("resolveSiteDirPath returned %q, %v; want relative fallback match", got, ok)
	}
	if !searched {
		t.Fatal("resolveSiteDirPath did not search for missing relative target")
	}
	if out.Len() != 0 {
		t.Fatalf("unexpected response: %q", out.String())
	}
}
