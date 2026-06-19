package master

import (
	"goftpd/internal/core"
	"goftpd/internal/protocol"
	"path/filepath"
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

func TestCacheSFVKeepsLiveRaceWindow(t *testing.T) {
	sm := NewSlaveManager("127.0.0.1", 1099, false, "", "", 60)
	bridge := &Bridge{sm: sm}
	sm.StartReleaseRaceWindowAt("/X265/release", 1000)

	bridge.CacheSFV("/X265/release", "release.sfv", core.SFVInfo{
		Entries: []core.SFVEntryInfo{{FileName: "file.r00", CRC32: 1}},
	})
	sm.NoteRacePayloadTransferAt("/X265/release", 100, 3000)

	// First payload defines the start (3000-100=2900), overriding the mkdir
	// seed at 1000, so the live window is 100ms and survives the SFV cache.
	if got := sm.GetReleaseRaceWindowMilliseconds("/X265/release"); got != 100 {
		t.Fatalf("race window after SFV cache = %dms, want 100ms", got)
	}
}

func TestReadFileMissingVFSEntryDoesNotProbeSlaves(t *testing.T) {
	sm := NewSlaveManager("127.0.0.1", 1099, false, "", "", 60)
	rs := &RemoteSlave{name: "LOCAL"}
	rs.online.Store(true)
	rs.available.Store(true)
	sm.slaves["LOCAL"] = rs

	bridge := &Bridge{sm: sm, readFileCache: make(map[string]cachedReadFileResult)}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("ReadFile should not try to send a readFile command for missing VFS entries: %v", r)
		}
	}()

	if _, err := bridge.ReadFile("/ARCHiVE/TV-1080P/.tvmaze"); err == nil {
		t.Fatalf("expected missing VFS file to return an error")
	}
}

func TestListDirRepairsZeroSizeFromVerifiedRaceDB(t *testing.T) {
	const checksum = 0x12345678
	const fullSize = int64(400000000)
	releasePath := "/X265/Release.Name-GRP"
	filePath := releasePath + "/release.name-grp.r15"

	sm := NewSlaveManager("127.0.0.1", 1099, false, "", "", 60)
	sm.GetVFS().AddFile(filePath, VFSFile{
		Path:         filePath,
		Size:         0,
		IsDir:        false,
		SlaveName:    "LOCAL",
		Owner:        "GoFTPd",
		Group:        "GoFTPd",
		XferTime:     1000,
		Checksum:     checksum,
		LastModified: 1234,
	})

	raceDB, err := NewRaceDB(filepath.Join(t.TempDir(), "race.db"))
	if err != nil {
		t.Fatalf("new race DB: %v", err)
	}
	defer raceDB.Close()
	if err := raceDB.SaveSFV(releasePath, "release.sfv", map[string]uint32{
		"release.name-grp.r15": checksum,
	}); err != nil {
		t.Fatalf("save SFV: %v", err)
	}
	if err := raceDB.RecordUpload(filePath, "steel", "iND", fullSize, 2500, checksum); err != nil {
		t.Fatalf("record upload: %v", err)
	}

	bridge := &Bridge{sm: sm, raceDB: raceDB, readFileCache: make(map[string]cachedReadFileResult)}
	entries := bridge.ListDir(releasePath)
	for _, entry := range entries {
		if entry.Name != "release.name-grp.r15" {
			continue
		}
		if entry.Size != fullSize {
			t.Fatalf("listing size = %d, want %d", entry.Size, fullSize)
		}
		if entry.Owner != "steel" || entry.Group != "iND" {
			t.Fatalf("listing owner/group = %s/%s, want steel/iND", entry.Owner, entry.Group)
		}
		return
	}
	t.Fatalf("expected listed file")
}
