package quota

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestScanAndProcessDisablesFailedTrial(t *testing.T) {
	root := t.TempDir()
	usersDir := filepath.Join(root, "users")
	byeDir := filepath.Join(root, "byefiles")
	stateFile := filepath.Join(root, "state.yml")
	if err := os.MkdirAll(usersDir, 0755); err != nil {
		t.Fatalf("mkdir users: %v", err)
	}
	if err := writeUserFile(filepath.Join(usersDir, "alice"), "GROUP iND\nFLAGS 3\nWKUP 1 0 0\nDAYUP 1 0 0\n"); err != nil {
		t.Fatalf("write user: %v", err)
	}

	p := New()
	p.usersDir = usersDir
	p.byeDir = byeDir
	p.stateFile = stateFile
	p.trialQuotaBytes = gibToBytes(1)
	p.trialDaysDefault = 1
	p.state.Users = map[string]*trackedUser{
		"alice": {
			Status:         statusTrial,
			TrialStartUnix: time.Now().Add(-48 * time.Hour).Unix(),
			TrialDays:      1,
			DaysRemaining:  0,
			IRCNick:        "AliceNick",
		},
	}

	var kicks []string
	p.kickChannels = []string{"#staff"}
	p.kickOnDisable = true
	p.kicker = func(channel, nick, reason string) {
		kicks = append(kicks, channel+"|"+nick+"|"+reason)
	}

	if err := p.scanAndProcess(); err != nil {
		t.Fatalf("scanAndProcess: %v", err)
	}

	if got := p.state.Users["alice"].Status; got != statusDisabled {
		t.Fatalf("expected disabled status, got %q", got)
	}
	content, err := os.ReadFile(filepath.Join(usersDir, "alice"))
	if err != nil {
		t.Fatalf("read user file: %v", err)
	}
	if !strings.Contains(string(content), "FLAGS 63") && !strings.Contains(string(content), "FLAGS 36") {
		t.Fatalf("expected disabled flag added, got:\n%s", string(content))
	}
	byeContent, err := os.ReadFile(filepath.Join(byeDir, "alice.bye"))
	if err != nil {
		t.Fatalf("read bye file: %v", err)
	}
	if !strings.Contains(string(byeContent), "Trial Failure") {
		t.Fatalf("expected trial failure in bye file, got:\n%s", string(byeContent))
	}
	if len(kicks) != 1 {
		t.Fatalf("expected 1 kick, got %d", len(kicks))
	}
}

func TestSetUserTrialRemovesDisabledFlag(t *testing.T) {
	root := t.TempDir()
	usersDir := filepath.Join(root, "users")
	if err := os.MkdirAll(usersDir, 0755); err != nil {
		t.Fatalf("mkdir users: %v", err)
	}
	userPath := filepath.Join(usersDir, "bob")
	if err := writeUserFile(userPath, "GROUP iND\nFLAGS 63\nWKUP 1 1024 0\nDAYUP 1 1024 0\n"); err != nil {
		t.Fatalf("write user: %v", err)
	}

	p := New()
	p.usersDir = usersDir
	p.byeDir = filepath.Join(root, "byefiles")
	p.stateFile = filepath.Join(root, "state.yml")
	p.state.Users = map[string]*trackedUser{}

	if err := p.setUserTrial("bob", 9); err != nil {
		t.Fatalf("setUserTrial: %v", err)
	}

	content, err := os.ReadFile(userPath)
	if err != nil {
		t.Fatalf("read user file: %v", err)
	}
	if strings.Contains(string(content), "FLAGS 63") || strings.Contains(string(content), "FLAGS 36") {
		t.Fatalf("expected disabled flag removed, got:\n%s", string(content))
	}
	state := p.state.Users["bob"]
	if state == nil || state.Status != statusTrial || state.TrialDays != 9 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func writeUserFile(path, content string) error {
	if !strings.HasSuffix(content, "\n") {
		content += "\n"
	}
	return os.WriteFile(path, []byte(content), 0644)
}
