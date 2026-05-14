package slave

import (
	"os"
	"path/filepath"
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

func TestScanTargetsForBaseIncludeArchiveMountAtRoot(t *testing.T) {
	s := &Slave{
		roots: []MountedRoot{
			{Path: "/site", MountPath: "/"},
			{Path: "/archive1", MountPath: "/ARCHiVE"},
		},
	}

	targets := s.scanTargetsForBase("/")
	if len(targets) != 2 {
		t.Fatalf("expected 2 scan targets, got %d", len(targets))
	}
	if targets[0].virtualBase != "/ARCHiVE" {
		t.Fatalf("expected more specific mount first, got %+v", targets)
	}
}
