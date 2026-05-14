package user

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadAndSavePreservesImportedUserfileFields(t *testing.T) {
	tmp := t.TempDir()
	oldWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd() error = %v", err)
	}
	defer func() { _ = os.Chdir(oldWD) }()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("Chdir() error = %v", err)
	}

	if err := os.MkdirAll(filepath.Join("etc", "users"), 0755); err != nil {
		t.Fatalf("MkdirAll(users) error = %v", err)
	}
	passwd := "Finity:$01020304$d631702386055e6797948aa58b4551b2ba70492a:100:300:0:/site:/bin/false\n"
	if err := os.WriteFile(filepath.Join("etc", "passwd"), []byte(passwd), 0600); err != nil {
		t.Fatalf("WriteFile(passwd) error = %v", err)
	}

	input := strings.Join([]string{
		"USER Added by glftpd",
		"GENERAL 0,120 -1 0 0",
		"LOGINS 16 0 6 10",
		"TIMEFRAME 0 0",
		"FLAGS 3",
		"TAGLINE No Tagline Set",
		"HOMEDIR /site",
		"DIR /",
		"ADDED 1712306777 glftpd",
		"EXPIRES 0",
		"CREDITS 245752440015 0",
		"RATIO 3 -1",
		"LOGINSLOTS 16",
		"MAXSIM 0",
		"UPLOADSLOTS 0",
		"DOWNLOADSLOTS 0",
		"WKLYALLOTMENT 0",
		"GROUPSLOTS 0 0",
		"ALLUP 962989 106564602869 283909879 0 0 0",
		"ALLDN 616476 61577907703 248884788 0 0 0",
		"WKUP 224 28231629 1588 0 0 0",
		"WKDN 1356 155321900 44062 0 0 0",
		"DAYUP 160 18238879 1217 0 0 0",
		"DAYDN 906 98911324 29735 0 0 0",
		"MONTHUP 1347 128797999 105168 0 0 0",
		"MONTHDN 5371 547778033 925506 0 0 0",
		"NUKE 1777936801 1069 2480594 0 0 0",
		"TIME 1602990 1778010002 0 235935",
		"GROUP Friends 0",
		"PRIMARY_GROUP iND",
		"GROUP iND 0",
		"IP *@5.186.48.*",
	}, "\n") + "\n"
	userPath := filepath.Join("etc", "users", "Finity")
	if err := os.WriteFile(userPath, []byte(input), 0600); err != nil {
		t.Fatalf("WriteFile(user) error = %v", err)
	}

	u, err := LoadUser("Finity", map[string]int{"iND": 300, "Friends": 301})
	if err != nil {
		t.Fatalf("LoadUser() error = %v", err)
	}
	if err := u.Save(); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	if u.UploadSlots != 10 {
		t.Fatalf("UploadSlots = %d, want 10 derived from LOGINS", u.UploadSlots)
	}
	if u.DownloadSlots != 6 {
		t.Fatalf("DownloadSlots = %d, want 6 derived from LOGINS", u.DownloadSlots)
	}
	if u.LoginSlots != 16 {
		t.Fatalf("LoginSlots = %d, want 16 derived from LOGINS", u.LoginSlots)
	}
	if u.MaxSim != 0 {
		t.Fatalf("MaxSim = %d, want 0", u.MaxSim)
	}

	out, err := os.ReadFile(userPath)
	if err != nil {
		t.Fatalf("ReadFile(saved user) error = %v", err)
	}
	text := string(out)
	checks := []string{
		"USER Added by glftpd",
		"GENERAL 0,120 -1 0 0",
		"LOGINS 16 0 6 10",
		"TIMEFRAME 0 0",
		"ADDED 1712306777 glftpd",
		"CREDITS 245752440015 0",
		"RATIO 3 -1",
		"LOGINSLOTS 16",
		"MAXSIM 0",
		"UPLOADSLOTS 10",
		"DOWNLOADSLOTS 6",
		"WKLYALLOTMENT 0",
		"GROUPSLOTS 0 0",
		"ALLUP 962989 106564602869 283909879 0 0 0",
		"NUKE 1777936801 1069 2480594 0 0 0",
		"TIME 1602990 1778010002 0 235935",
		"PRIMARY_GROUP iND",
		"GROUP Friends 0",
		"GROUP iND 0",
	}
	for _, needle := range checks {
		if !strings.Contains(text, needle) {
			t.Fatalf("saved userfile missing %q\n%s", needle, text)
		}
	}
}

func TestResetTransferStatPeriodsIfDueUsesDedicatedPeriodAnchor(t *testing.T) {
	u := &User{
		LastLogin:    time.Date(2026, 5, 13, 20, 0, 0, 0, time.Local).Unix(),
		PeriodAnchor: time.Date(2026, 5, 13, 23, 55, 0, 0, time.Local).Unix(),
		DayUp:        StatLine{Files: 10, Bytes: 1000},
		WkUp:         StatLine{Files: 20, Bytes: 2000},
		MonthUp:      StatLine{Files: 30, Bytes: 3000},
	}

	now := time.Date(2026, 5, 14, 0, 5, 0, 0, time.Local)
	if !u.ResetTransferStatPeriodsIfDue(now) {
		t.Fatalf("expected rollover to reset stats")
	}
	if u.DayUp.Files != 0 || u.DayUp.Bytes != 0 {
		t.Fatalf("day stats were not reset: %+v", u.DayUp)
	}
	if u.PeriodAnchor != now.Unix() {
		t.Fatalf("PeriodAnchor = %d, want %d", u.PeriodAnchor, now.Unix())
	}
	if u.LastLogin != time.Date(2026, 5, 13, 20, 0, 0, 0, time.Local).Unix() {
		t.Fatalf("LastLogin should remain unchanged")
	}
}
