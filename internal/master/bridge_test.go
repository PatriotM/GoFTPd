package master

import (
	"goftpd/internal/core"
	"goftpd/internal/protocol"
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

func TestFinalUploadFileSizePrefersSlaveStatSize(t *testing.T) {
	status := protocol.TransferStatus{Transferred: 2 * 1024 * 1024, FileSize: 4892 * 1024}
	if got := finalUploadFileSize(status, 0); got != 4892*1024 {
		t.Fatalf("expected stat size to win, got %d", got)
	}
}

func TestFinalUploadFileSizeFallsBackToResumeOffset(t *testing.T) {
	status := protocol.TransferStatus{Transferred: 1024}
	if got := finalUploadFileSize(status, 2048); got != 3072 {
		t.Fatalf("expected transferred plus offset fallback, got %d", got)
	}
}
