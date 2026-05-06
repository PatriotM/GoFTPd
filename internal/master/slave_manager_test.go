package master

import (
	"testing"
	"time"

	"goftpd/internal/protocol"
)

func TestRemoteSlaveOfflineClearsVFSFiles(t *testing.T) {
	sm := NewSlaveManager("127.0.0.1", 1099, false, "", "", 60*time.Second)
	sm.vfs.AddFile("/X265/release", VFSFile{IsDir: true, Seen: true, SlaveName: "LOCAL"})
	sm.vfs.AddFile("/X265/release/file.r00", VFSFile{Seen: true, SlaveName: "LOCAL", Size: 100})
	sm.vfs.SetSFVData("/X265/release", "release.sfv", map[string]uint32{"file.r00": 1})

	rs := NewRemoteSlave("LOCAL", nil, nil, 60*time.Second, func(name string) {
		sm.vfs.ClearSlave(name)
	})
	rs.SetOffline("test")

	if got := sm.vfs.GetFile("/X265/release/file.r00"); got != nil {
		t.Fatalf("expected offline slave file to be cleared from VFS, got %+v", got)
	}
	if got := sm.vfs.GetFile("/X265/release"); got != nil {
		t.Fatalf("expected offline slave directory to be cleared from VFS, got %+v", got)
	}
	if meta := sm.vfs.GetSFVData("/X265/release"); meta != nil {
		t.Fatalf("expected offline slave metadata to be cleared from VFS, got %+v", meta)
	}
}

func TestShouldRefreshRemergeChecksumForTrackedUnverifiedPayload(t *testing.T) {
	sm := NewSlaveManager("127.0.0.1", 1099, false, "", "", 60*time.Second)
	sm.SetEnableRemergeChecksums(true)
	sm.vfs.AddFile("/X265/release", VFSFile{IsDir: true, Seen: true, SlaveName: "LOCAL"})
	sm.vfs.SetSFVData("/X265/release", "release.sfv", map[string]uint32{
		"file.r00": 1,
	})
	sm.vfs.AddFile("/X265/release/file.r00", VFSFile{
		Size:      100,
		Seen:      true,
		SlaveName: "LOCAL",
		Checksum:  0,
	})

	if !sm.shouldRefreshRemergeChecksum("/X265/release/file.r00", protocol.LightRemoteInode{Name: "file.r00", Size: 100}) {
		t.Fatalf("expected tracked unverified payload to request a remerge checksum refresh")
	}

	sm.vfs.UpdateFileVerification("/X265/release/file.r00", 1)
	if sm.shouldRefreshRemergeChecksum("/X265/release/file.r00", protocol.LightRemoteInode{Name: "file.r00", Size: 100}) {
		t.Fatalf("expected already verified payload to skip remerge checksum refresh")
	}

	if sm.shouldRefreshRemergeChecksum("/X265/release/file.nfo", protocol.LightRemoteInode{Name: "file.nfo", Size: 100}) {
		t.Fatalf("expected untracked side file to skip remerge checksum refresh")
	}
}

func TestShouldRefreshRemergeChecksumDisabledByDefault(t *testing.T) {
	sm := NewSlaveManager("127.0.0.1", 1099, false, "", "", 60*time.Second)
	sm.vfs.AddFile("/X265/release", VFSFile{IsDir: true, Seen: true, SlaveName: "LOCAL"})
	sm.vfs.SetSFVData("/X265/release", "release.sfv", map[string]uint32{
		"file.r00": 1,
	})
	sm.vfs.AddFile("/X265/release/file.r00", VFSFile{
		Size:      100,
		Seen:      true,
		SlaveName: "LOCAL",
		Checksum:  0,
	})

	if sm.shouldRefreshRemergeChecksum("/X265/release/file.r00", protocol.LightRemoteInode{Name: "file.r00", Size: 100}) {
		t.Fatalf("expected remerge checksum refresh to stay disabled by default")
	}
}

func TestSetRemergeFlowControlNormalizesThresholds(t *testing.T) {
	sm := NewSlaveManager("127.0.0.1", 1099, false, "", "", 60*time.Second)
	sm.SetRemergeFlowControl(0, 999)

	pauseThreshold, resumeThreshold := sm.GetRemergeFlowControl()
	if pauseThreshold != 250 {
		t.Fatalf("expected default pause threshold 250, got %d", pauseThreshold)
	}
	if resumeThreshold != 125 {
		t.Fatalf("expected normalized resume threshold 125, got %d", resumeThreshold)
	}
}

func TestSlaveAuthGuardBansAfterConfiguredFailures(t *testing.T) {
	sm := NewSlaveManager("127.0.0.1", 1099, false, "", "", 60*time.Second)
	sm.ConfigureAuthGuard(2, time.Minute, 10*time.Minute)

	sm.recordAuthFailure("1.2.3.4", "1.2.3.4:1234", "unexpected EOF")
	if banned, _ := sm.isAuthBanned("1.2.3.4"); banned {
		t.Fatalf("IP should not be banned after first failed handshake")
	}

	sm.recordAuthFailure("1.2.3.4", "1.2.3.4:1234", "unexpected EOF")
	if banned, _ := sm.isAuthBanned("1.2.3.4"); !banned {
		t.Fatalf("IP should be banned after reaching the failure limit")
	}
}

func TestSlaveAuthGuardClearsOnSuccessfulSlaveLogin(t *testing.T) {
	sm := NewSlaveManager("127.0.0.1", 1099, false, "", "", 60*time.Second)
	sm.ConfigureAuthGuard(2, time.Minute, 10*time.Minute)

	sm.recordAuthFailure("1.2.3.4", "1.2.3.4:1234", "unexpected EOF")
	sm.clearAuthState("1.2.3.4")

	if banned, _ := sm.isAuthBanned("1.2.3.4"); banned {
		t.Fatalf("IP should not remain banned after state is cleared")
	}
}

func TestSlaveAuthAllowlistSupportsExactIPsAndCIDRs(t *testing.T) {
	sm := NewSlaveManager("127.0.0.1", 1099, false, "", "", 60*time.Second)
	if err := sm.ConfigureAuthAllowlist([]string{"1.2.3.4", "10.0.0.0/8"}); err != nil {
		t.Fatalf("ConfigureAuthAllowlist returned error: %v", err)
	}

	if !sm.isAuthAllowed("1.2.3.4") {
		t.Fatalf("exact allowlist IP should be allowed")
	}
	if !sm.isAuthAllowed("10.5.6.7") {
		t.Fatalf("CIDR allowlist IP should be allowed")
	}
	if sm.isAuthAllowed("8.8.8.8") {
		t.Fatalf("non-allowlisted IP should not be allowed")
	}
}

func TestSlaveAuthDenylistAddRemove(t *testing.T) {
	dir := t.TempDir()
	sm := NewSlaveManager("127.0.0.1", 1099, false, "", "", 60*time.Second)
	if err := sm.ConfigureAuthDenylistFile(dir + "/slave_denylist.txt"); err != nil {
		t.Fatalf("ConfigureAuthDenylistFile returned error: %v", err)
	}

	entry, err := sm.AddAuthDenyEntry("1.2.3.4")
	if err != nil {
		t.Fatalf("AddAuthDenyEntry returned error: %v", err)
	}
	if entry != "1.2.3.4" {
		t.Fatalf("unexpected canonical entry %q", entry)
	}

	if denied, _ := sm.isAuthExplicitlyDenied("1.2.3.4"); !denied {
		t.Fatalf("expected IP to be denylisted")
	}

	removed, err := sm.RemoveAuthDenyEntry("1.2.3.4")
	if err != nil {
		t.Fatalf("RemoveAuthDenyEntry returned error: %v", err)
	}
	if !removed {
		t.Fatalf("expected denylist entry to be removed")
	}

	if denied, _ := sm.isAuthExplicitlyDenied("1.2.3.4"); denied {
		t.Fatalf("expected IP to be removed from denylist")
	}
}
