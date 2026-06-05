package zipscript

import (
	"errors"
	"testing"
)

type fakeSFVRuntimeBridge struct {
	sfvEntries map[string]uint32
	checksums  map[string]uint32
	files      map[string]bool
	deleteErr  error
	writes     []string
	deletes    []string
	missing    []string
}

func (b *fakeSFVRuntimeBridge) GetSFVData(dirPath string) map[string]uint32 {
	return b.sfvEntries
}

func (b *fakeSFVRuntimeBridge) GetKnownChecksum(filePath string) (uint32, bool) {
	checksum, ok := b.checksums[filePath]
	return checksum, ok
}

func (b *fakeSFVRuntimeBridge) DeleteFile(filePath string) error {
	b.deletes = append(b.deletes, filePath)
	if b.deleteErr != nil {
		return b.deleteErr
	}
	delete(b.files, filePath)
	return nil
}

func (b *fakeSFVRuntimeBridge) MarkFileMissing(filePath string) error {
	b.missing = append(b.missing, filePath)
	return nil
}

func (b *fakeSFVRuntimeBridge) GetFileSize(filePath string) int64 {
	if b.files[filePath] {
		return 1
	}
	return -1
}

func (b *fakeSFVRuntimeBridge) WriteFile(filePath string, data []byte) error {
	b.writes = append(b.writes, filePath)
	b.files[filePath] = true
	return nil
}

func TestShouldTreatDownloadAsMissingCreatesMarkerWithoutDeleting(t *testing.T) {
	cfg := Config{
		Enabled: true,
		Sections: SectionsConfig{
			SFV: []string{"/X265/*"},
		},
		List: ListConfig{
			MissingFiles: boolPtr(true),
		},
	}
	bridge := &fakeSFVRuntimeBridge{
		sfvEntries: map[string]uint32{"file.r00": 0x12345678},
		checksums:  map[string]uint32{"/X265/release/file.r00": 0x87654321},
		files:      map[string]bool{"/X265/release/file.r00": true},
	}

	if !ShouldTreatDownloadAsMissing(cfg, bridge, "/X265/release/file.r00", nil) {
		t.Fatalf("expected bad known checksum to be treated as missing")
	}
	if len(bridge.writes) != 1 || bridge.writes[0] != "/X265/release/file.r00-MISSING" {
		t.Fatalf("expected missing marker write, got %#v", bridge.writes)
	}
	if len(bridge.deletes) != 0 || len(bridge.missing) != 0 {
		t.Fatalf("did not expect delete/MarkFileMissing when delete-bad is disabled, deletes=%#v missing=%#v", bridge.deletes, bridge.missing)
	}
}

func TestShouldTreatDownloadAsMissingIgnoresAlreadyDeletedBadFile(t *testing.T) {
	cfg := Config{
		Enabled: true,
		Sections: SectionsConfig{
			SFV: []string{"/X265/*"},
		},
		List: ListConfig{
			MissingFiles: boolPtr(true),
		},
		SFV: SFVConfig{
			DeleteBadCRC: true,
		},
	}
	bridge := &fakeSFVRuntimeBridge{
		sfvEntries: map[string]uint32{"file.r00": 0x12345678},
		checksums:  map[string]uint32{"/X265/release/file.r00": 0x87654321},
		files:      map[string]bool{"/X265/release/file.r00": true},
		deleteErr:  errors.New("remove /site/file.r00: no such file or directory"),
	}
	var logs []string

	if !ShouldTreatDownloadAsMissing(cfg, bridge, "/X265/release/file.r00", func(format string, args ...any) {
		logs = append(logs, format)
	}) {
		t.Fatalf("expected bad known checksum to be treated as missing")
	}
	if len(logs) != 0 {
		t.Fatalf("expected not-found delete errors to be quiet, got %#v", logs)
	}
	if len(bridge.deletes) != 1 || bridge.deletes[0] != "/X265/release/file.r00" {
		t.Fatalf("expected delete attempt, got %#v", bridge.deletes)
	}
	if len(bridge.missing) != 1 || bridge.missing[0] != "/X265/release/file.r00" {
		t.Fatalf("expected MarkFileMissing despite missing disk file, got %#v", bridge.missing)
	}
}
