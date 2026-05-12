package master

import (
	"goftpd/internal/core"
	"strings"
	"testing"
)

func TestNukeVirtualEntriesFromHistory(t *testing.T) {
	entry := &core.NukeHistoryEntry{
		Multiplier: 10,
		Reason:     "-Auto- Incomplete",
		NukedBy:    "goftpd",
		Nukees:     "Neptun,probe",
		NukedAt:    12345,
	}

	entries := nukeVirtualEntriesFromHistory(entry)
	if len(entries) != 3 {
		t.Fatalf("expected 3 virtual nuke entries, got %d", len(entries))
	}

	var sawReason, sawNukees, sawNuker bool
	for _, e := range entries {
		if !e.IsDir {
			t.Fatalf("expected virtual nuke entry %q to be a directory marker", e.Name)
		}
		switch {
		case strings.Contains(e.Name, "!NUKE") && strings.Contains(e.Name, "x10") && strings.Contains(e.Name, "Incomplete"):
			sawReason = true
		case strings.Contains(e.Name, "!NUKEES") && strings.Contains(e.Name, "Neptun,probe"):
			sawNukees = true
		case strings.Contains(e.Name, "!NUKER") && strings.Contains(e.Name, "goftpd"):
			sawNuker = true
		}
	}

	if !sawReason {
		t.Fatalf("expected reason marker entry")
	}
	if !sawNukees {
		t.Fatalf("expected nukees marker entry")
	}
	if !sawNuker {
		t.Fatalf("expected nuker marker entry")
	}
}

