package acl

import (
	"testing"

	"goftpd/internal/user"
)

func TestMatchesRulePathCoversPrivatePathDescendants(t *testing.T) {
	e := &Engine{RulesByType: map[string][]Rule{
		"privpath": {
			{Type: "privpath", Path: "/site/PRE/GROUP", Required: "=GROUP =SiteOP"},
			{Type: "privpath", Path: "/site/LINKS/*", Required: "1"},
		},
	}}

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "exact", path: "/site/PRE/GROUP", want: true},
		{name: "child release", path: "/site/PRE/GROUP/Release-GRP", want: true},
		{name: "nested file", path: "/site/PRE/GROUP/Release-GRP/file.rar", want: true},
		{name: "sibling group", path: "/site/PRE/OTHER/Release-GRP", want: false},
		{name: "wildcard", path: "/site/LINKS/something", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := e.MatchesRulePath("privpath", tt.path)
			if got != tt.want {
				t.Fatalf("MatchesRulePath(privpath, %q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}

func TestCanPerformCoversExactPathDescendants(t *testing.T) {
	e := &Engine{RulesByType: map[string][]Rule{
		"dirlog": {
			{Type: "dirlog", Path: "/site/MP3", Required: "*"},
			{Type: "dirlog", Path: "/site/PRE/GROUP", Required: "=GROUP =SiteOP"},
		},
	}}

	u := &user.User{
		Name:         "tester",
		PrimaryGroup: "GROUP",
		Groups:       map[string]int{"GROUP": 0},
	}

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "exact root", path: "/site/MP3", want: true},
		{name: "dated dir", path: "/site/MP3/0424", want: true},
		{name: "release below exact rule", path: "/site/MP3/0424/Artist-Album-2026-GRP", want: true},
		{name: "private exact path descendant", path: "/site/PRE/GROUP/Release-GRP", want: true},
		{name: "private nested descendant", path: "/site/PRE/GROUP/Release-GRP/file.rar", want: true},
		{name: "outside subtree", path: "/site/FLAC/0424/Artist-Album-2026-GRP", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := e.CanPerform(u, "DIRLOG", tt.path)
			if got != tt.want {
				t.Fatalf("CanPerform(DIRLOG, %q) = %v, want %v", tt.path, got, tt.want)
			}
		})
	}
}
