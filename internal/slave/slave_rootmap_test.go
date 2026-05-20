package slave

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRootsForVirtualPathPrefersSpecificMount(t *testing.T) {
	s := &Slave{
		roots: []MountedRoot{
			{Path: "/site", MountPath: "/"},
			{Path: "/archive1", MountPath: "/ARCHiVE"},
			{Path: "/archive2", MountPath: "/ARCHiVE"},
		},
	}

	got := s.rootsForVirtualPath("/ARCHiVE/EBOOKS/release")
	if len(got) != 3 {
		t.Fatalf("expected 3 matching roots, got %d", len(got))
	}
	if got[0].MountPath != "/ARCHiVE" || got[1].MountPath != "/ARCHiVE" {
		t.Fatalf("expected archive mounts first, got %+v", got)
	}
}

func TestGetDirForUploadUsesMountedArchiveRoots(t *testing.T) {
	siteRoot := t.TempDir()
	archiveRoot := t.TempDir()

	if err := os.MkdirAll(filepath.Join(archiveRoot, "EBOOKS", "0514"), 0o755); err != nil {
		t.Fatalf("mkdir archive: %v", err)
	}

	s := &Slave{
		roots: []MountedRoot{
			{Path: siteRoot, MountPath: "/"},
			{Path: archiveRoot, MountPath: "/ARCHiVE"},
		},
	}

	fullPath, err := s.getDirForUpload("/ARCHiVE/EBOOKS/0514/release")
	if err != nil {
		t.Fatalf("getDirForUpload failed: %v", err)
	}
	want := filepath.Join(archiveRoot, "EBOOKS", "0514", "release")
	if fullPath != want {
		t.Fatalf("expected %s, got %s", want, fullPath)
	}
}

func TestGetDirForUploadRejectsRootBelowFreeSpaceThreshold(t *testing.T) {
	siteRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(siteRoot, "TV-1080P"), 0o755); err != nil {
		t.Fatalf("mkdir site root: %v", err)
	}
	avail, _ := getDiskSpace(siteRoot)
	if avail <= 0 {
		t.Skip("filesystem free space is unavailable")
	}

	s := &Slave{
		roots: []MountedRoot{
			{Path: siteRoot, MountPath: "/"},
		},
		freeSpaceMB: int(avail/(1024*1024)) + 1,
	}

	_, err := s.getDirForUpload("/TV-1080P/release/file.r00")
	if err == nil {
		t.Fatalf("expected upload to be rejected below free_space_mb")
	}
	if !strings.Contains(err.Error(), "free_space_mb") {
		t.Fatalf("expected free_space_mb error, got %v", err)
	}
}

func TestScanTargetsForBaseIncludeArchiveMountAtRoot(t *testing.T) {
	s := &Slave{
		roots: []MountedRoot{
			{Path: "/site", MountPath: "/"},
			{Path: "/archive1", MountPath: "/ARCHiVE"},
		},
	}

	targets := s.scanTargetsForBase("/", false)
	if len(targets) != 2 {
		t.Fatalf("expected 2 scan targets, got %d", len(targets))
	}
	if targets[0].virtualBase != "/" || targets[1].virtualBase != "/ARCHiVE" {
		t.Fatalf("expected site root first at / remerge, got %+v", targets)
	}
}

func TestWaitForRemergeSlotStopsOnAbort(t *testing.T) {
	s := &Slave{}
	s.online.Store(true)
	s.remergeAbort.Store(true)

	if s.waitForRemergeSlot() {
		t.Fatalf("expected remerge slot wait to stop when abort is requested")
	}
}
