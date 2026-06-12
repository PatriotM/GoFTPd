package slowkick

import (
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"goftpd/internal/plugin"
	"goftpd/internal/user"
)

func testServices() *plugin.Services {
	return &plugin.Services{
		Logger: log.New(os.Stderr, "", 0),
		ListActiveSessions: func() []plugin.ActiveSession {
			return []plugin.ActiveSession{
				{ID: 1, User: "tester", LoggedIn: true},
				{ID: 2, User: "other", LoggedIn: true},
			}
		},
	}
}

func TestTransferSpeedPolicyUploadUsesConfiguredFloorAndGrace(t *testing.T) {
	h := New()
	h.svc = testServices()
	h.monitorUploads = true
	h.minUsersOnline = 2
	h.minUploadSpeedBytes = 5 * 1024
	h.uploadGrace = 7 * time.Second

	minSpeed, maxSpeed, grace, ok := h.TransferSpeedPolicy("tester", "USERS", "/TV/release/file.r00", "upload")
	if !ok {
		t.Fatal("expected upload policy to apply")
	}
	if minSpeed != 5*1024 {
		t.Fatalf("expected min speed 5120, got %d", minSpeed)
	}
	if maxSpeed != 0 {
		t.Fatalf("expected no max speed, got %d", maxSpeed)
	}
	if grace != 7 {
		t.Fatalf("expected grace 7s, got %d", grace)
	}
}

func TestTransferSpeedPolicySkipsExcludedExtension(t *testing.T) {
	h := New()
	h.svc = testServices()
	h.monitorUploads = true
	h.minUsersOnline = 2
	h.minUploadSpeedBytes = 5 * 1024
	h.excludeExtensions = lowerSet([]string{"sfv", "nfo"})

	if _, _, _, ok := h.TransferSpeedPolicy("tester", "USERS", "/XXX/release/release.nfo", "upload"); ok {
		t.Fatal("expected excluded extension to skip policy")
	}
}

func TestTransferSpeedPolicySkipsWhenUsersBelowMinimum(t *testing.T) {
	h := New()
	h.svc = &plugin.Services{
		Logger: log.New(os.Stderr, "", 0),
		ListActiveSessions: func() []plugin.ActiveSession {
			return []plugin.ActiveSession{{ID: 1, User: "tester", LoggedIn: true}}
		},
	}
	h.monitorUploads = true
	h.minUsersOnline = 2
	h.minUploadSpeedBytes = 5 * 1024

	if _, _, _, ok := h.TransferSpeedPolicy("tester", "USERS", "/TV/release/file.r00", "upload"); ok {
		t.Fatal("expected policy to stay disabled below min users")
	}
}

func TestHandleSlowTransferSetsTempBanAndEmitsKick(t *testing.T) {
	var eventType string
	var eventPath string
	var eventData map[string]string

	h := New()
	h.svc = testServices()
	h.svc.EmitEvent = func(evtType, evtPath, filename, section string, size int64, speed float64, data map[string]string) {
		eventType = evtType
		eventPath = evtPath
		eventData = data
	}
	h.monitorUploads = true
	h.minUsersOnline = 2
	h.minUploadSpeedBytes = 5 * 1024
	h.announceKick = true
	h.tempbanAfterKick = true
	h.tempbanDuration = 5 * time.Second

	h.HandleSlowTransfer("tester", "USERS", "/TV/release/file.r00", "upload", "LOCAL", 42, 1024, 5*1024)

	if eventType != "SLOWUPLOADKICK" {
		t.Fatalf("expected SLOWUPLOADKICK event, got %q", eventType)
	}
	if eventPath != "/TV/release/file.r00" {
		t.Fatalf("expected event path to match transfer path, got %q", eventPath)
	}
	if eventData["tempban_seconds"] != "5" {
		t.Fatalf("expected tempban_seconds=5, got %+v", eventData)
	}
	if err := h.ValidateLogin(&user.User{Name: "tester", PrimaryGroup: "USERS"}, "127.0.0.1"); err == nil {
		t.Fatal("expected kicked user to be tempbanned")
	}
}

func TestHandleSlowTransferSkipsExcludedPath(t *testing.T) {
	var emitted bool

	h := New()
	h.svc = testServices()
	h.svc.EmitEvent = func(evtType, evtPath, filename, section string, size int64, speed float64, data map[string]string) {
		emitted = true
	}
	h.monitorUploads = true
	h.minUsersOnline = 2
	h.minUploadSpeedBytes = 5 * 1024
	h.announceKick = true
	h.tempbanAfterKick = true
	h.tempbanDuration = 5 * time.Second

	h.HandleSlowTransfer("tester", "USERS", "/REQUESTS/Test/file.r00", "upload", "LOCAL", 42, 1024, 5*1024)

	if emitted {
		t.Fatal("did not expect event for excluded path")
	}
	if err := h.ValidateLogin(&user.User{Name: "tester", PrimaryGroup: "USERS"}, "127.0.0.1"); err != nil {
		t.Fatalf("did not expect excluded path to tempban user, got %v", err)
	}
}

func TestValidateLoginAllowsUserAfterTempBanExpires(t *testing.T) {
	h := New()
	h.tempbanAfterKick = true
	h.tempbanDuration = 15 * time.Second
	h.excludeUsers = map[string]struct{}{}
	h.excludeGroups = map[string]struct{}{}
	h.tempBans = map[string]time.Time{
		"slowpoke": time.Now().Add(-time.Second),
	}

	if err := h.ValidateLogin(&user.User{Name: "slowpoke", PrimaryGroup: "USERS"}, "127.0.0.1"); err != nil {
		t.Fatalf("expected expired tempban to be ignored, got %v", err)
	}
}

func TestValidateLoginReturnsRemainingSeconds(t *testing.T) {
	h := New()
	h.tempbanAfterKick = true
	h.tempbanDuration = 15 * time.Second
	h.excludeUsers = map[string]struct{}{}
	h.excludeGroups = map[string]struct{}{}
	h.tempBans = map[string]time.Time{
		"slowpoke": time.Now().Add(5 * time.Second),
	}

	err := h.ValidateLogin(&user.User{Name: "slowpoke", PrimaryGroup: "USERS"}, "127.0.0.1")
	if err == nil {
		t.Fatal("expected active tempban to reject login")
	}
	if !strings.Contains(err.Error(), "retry in") {
		t.Fatalf("expected retry hint in error, got %v", err)
	}
}

func TestReloadConfigCanDisableSlowkick(t *testing.T) {
	h := New()
	h.svc = testServices()
	h.setTempBan("tester", time.Now().Add(time.Minute))

	if err := h.ReloadConfig(map[string]interface{}{
		"enabled":          false,
		"min_users_online": 0,
	}); err != nil {
		t.Fatalf("ReloadConfig failed: %v", err)
	}

	if _, _, _, ok := h.TransferSpeedPolicy("tester", "USERS", "/TV/release/file.r00", "upload"); ok {
		t.Fatal("expected disabled slowkick to return no transfer policy")
	}
	if err := h.ValidateLogin(&user.User{Name: "tester", PrimaryGroup: "USERS"}, "127.0.0.1"); err != nil {
		t.Fatalf("expected disabled slowkick to ignore tempban, got %v", err)
	}
}

func TestReloadConfigCanClearDefaultExclusions(t *testing.T) {
	h := New()
	h.svc = testServices()

	if _, _, _, ok := h.TransferSpeedPolicy("tester", "USERS", "/REQUESTS/Test/file.sfv", "upload"); ok {
		t.Fatal("expected default request/sfv exclusions before reload")
	}

	if err := h.ReloadConfig(map[string]interface{}{
		"monitor_uploads":         true,
		"min_upload_speed_kbps":   5,
		"min_users_online":        0,
		"exclude_paths":           []interface{}{},
		"exclude_extensions":      []interface{}{},
		"tempban_after_kick":      false,
		"verify_upload_seconds":   3,
		"verify_download_seconds": 3,
	}); err != nil {
		t.Fatalf("ReloadConfig failed: %v", err)
	}

	minSpeed, _, grace, ok := h.TransferSpeedPolicy("tester", "USERS", "/REQUESTS/Test/file.sfv", "upload")
	if !ok {
		t.Fatal("expected cleared exclusions to allow upload policy")
	}
	if minSpeed != 5*1024 {
		t.Fatalf("expected reloaded min speed 5120, got %d", minSpeed)
	}
	if grace != 3 {
		t.Fatalf("expected reloaded grace 3, got %d", grace)
	}
}
