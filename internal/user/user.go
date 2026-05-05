package user

import (
	"fmt"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// StatLine matches userfile format: Files, Bytes, Meta
type StatLine struct {
	Files int64 `yaml:"files"`
	Bytes int64 `yaml:"bytes"`
	Meta  int64 `yaml:"meta"`
}

type User struct {
	Name         string         `yaml:"name"`
	Password     string         `yaml:"password"`
	UID          int            `yaml:"uid"`
	GID          int            `yaml:"gid"`
	Flags        string         `yaml:"flags"`
	Tagline      string         `yaml:"tagline"`
	HomeRoot     string         `yaml:"home_root"`
	HomeDir      string         `yaml:"homedir"`
	CurrentDir   string         `yaml:"current_dir"` // Runtime: current FTP dir
	Added        int64          `yaml:"added"`
	LastLogin    int64          `yaml:"last_login"`
	Expires      int64          `yaml:"expires"`
	Credits      int64          `yaml:"credits"`
	Ratio        int            `yaml:"ratio"`
	Groups       map[string]int `yaml:"groups"`
	PrimaryGroup string         `yaml:"primary_group"` // Primary group for file ownership
	IPs          []string       `yaml:"ips"`

	// Throughput Stats (files, bytes, meta)
	AllUp   StatLine `yaml:"allup"`
	AllDn   StatLine `yaml:"alldn"`
	WkUp    StatLine `yaml:"wkup"`
	WkDn    StatLine `yaml:"wkdn"`
	DayUp   StatLine `yaml:"dayup"`
	DayDn   StatLine `yaml:"daydn"`
	MonthUp StatLine `yaml:"monthup"`
	MonthDn StatLine `yaml:"monthdn"`

	// Nuke Stats
	NukeStat StatLine `yaml:"nukestat"`

	// Slot Configuration
	UploadSlots      int  `yaml:"upload_slots"`   // Max concurrent uploads
	DownloadSlots    int  `yaml:"download_slots"` // Max concurrent downloads
	UploadSlotsSet   bool `yaml:"-"`
	DownloadSlotsSet bool `yaml:"-"`

	// Raw userfile fields we preserve across load/save so imported glFTPD
	// accounts keep their original shape instead of being flattened.
	UserLine      string            `yaml:"-"`
	GeneralLine   string            `yaml:"-"`
	LoginsLine    string            `yaml:"-"`
	TimeframeLine string            `yaml:"-"`
	AddedBy       string            `yaml:"-"`
	CreditsExtra  string            `yaml:"-"`
	RatioExtra    string            `yaml:"-"`
	StatExtras    map[string]string `yaml:"-"`
	TimeFields    []string          `yaml:"-"`
}

// LoadUser reads user file - supports userfile format
func LoadUser(name string, groupMap map[string]int) (*User, error) {
	// Use exact case - usernames are case-sensitive like goftpd
	path := filepath.Join("etc", "users", name)
	return loadUserFile(name, path, groupMap)
}

func LoadTemplate(name, templatePath string, groupMap map[string]int) (*User, error) {
	return loadUserFile(name, templatePath, groupMap)
}

func loadUserFile(name, path string, groupMap map[string]int) (*User, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Parse userfile format
	u := &User{
		Name:       name, // Keep original case
		Groups:     make(map[string]int),
		IPs:        []string{},
		UID:        1000, // default
		GID:        300,  // default
		StatExtras: make(map[string]string),
	}

	// Load UID/GID from passwd file
	if passwdData, err := os.ReadFile("etc/passwd"); err == nil {
		lines := strings.Split(string(passwdData), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			parts := strings.Split(line, ":")
			if len(parts) >= 4 && parts[0] == name {
				fmt.Sscanf(parts[2], "%d", &u.UID)
				fmt.Sscanf(parts[3], "%d", &u.GID)
				break
			}
		}
	}

	// Simple goftpd parser inline
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		cmd := parts[0]
		switch cmd {
		case "USER":
			if len(parts) > 1 {
				u.UserLine = strings.Join(parts[1:], " ")
			}
		case "GENERAL":
			if len(parts) > 1 {
				u.GeneralLine = strings.Join(parts[1:], " ")
			}
		case "LOGINS":
			if len(parts) > 1 {
				u.LoginsLine = strings.Join(parts[1:], " ")
			}
		case "HOMEDIR":
			if len(parts) > 1 {
				u.HomeRoot = parts[1]
			}
		case "FLAGS":
			if len(parts) > 1 {
				u.Flags = parts[1]
			}
		case "TAGLINE":
			if len(parts) > 1 {
				u.Tagline = strings.Join(parts[1:], " ")
			}
		case "DIR":
			if len(parts) > 1 {
				u.HomeDir = parts[1]
			}
		case "RATIO":
			if len(parts) > 1 {
				fmt.Sscanf(parts[1], "%d", &u.Ratio)
			}
			if len(parts) > 2 {
				u.RatioExtra = strings.Join(parts[2:], " ")
			}
		case "CREDITS":
			if len(parts) > 1 {
				fmt.Sscanf(parts[1], "%d", &u.Credits)
			}
			if len(parts) > 2 {
				u.CreditsExtra = strings.Join(parts[2:], " ")
			}
		case "ALLUP", "ALLDN", "WKUP", "WKDN", "DAYUP", "DAYDN", "MONTHUP", "MONTHDN":
			if len(parts) >= 4 {
				var files, bytes, meta int64
				fmt.Sscanf(parts[1], "%d", &files)
				fmt.Sscanf(parts[2], "%d", &bytes)
				fmt.Sscanf(parts[3], "%d", &meta)
				stat := StatLine{Files: files, Bytes: bytes, Meta: meta}

				switch cmd {
				case "ALLUP":
					u.AllUp = stat
				case "ALLDN":
					u.AllDn = stat
				case "WKUP":
					u.WkUp = stat
				case "WKDN":
					u.WkDn = stat
				case "DAYUP":
					u.DayUp = stat
				case "DAYDN":
					u.DayDn = stat
				case "MONTHUP":
					u.MonthUp = stat
				case "MONTHDN":
					u.MonthDn = stat
				}
				if len(parts) > 4 {
					u.StatExtras[cmd] = strings.Join(parts[4:], " ")
				}
			}
		case "NUKE":
			if len(parts) >= 4 {
				var last, times, bytes int64
				fmt.Sscanf(parts[1], "%d", &last)
				fmt.Sscanf(parts[2], "%d", &times)
				fmt.Sscanf(parts[3], "%d", &bytes)
				u.NukeStat = StatLine{Files: times, Bytes: bytes, Meta: last}
				if len(parts) > 4 {
					u.StatExtras[cmd] = strings.Join(parts[4:], " ")
				}
			}
		case "TIME":
			if len(parts) > 1 {
				u.TimeFields = append([]string(nil), parts[1:]...)
			}
			if len(parts) >= 3 {
				var lastOn int64
				fmt.Sscanf(parts[2], "%d", &lastOn)
				u.LastLogin = lastOn
			}
		case "ADDED":
			if len(parts) > 1 {
				fmt.Sscanf(parts[1], "%d", &u.Added)
			}
			if len(parts) > 2 {
				u.AddedBy = strings.Join(parts[2:], " ")
			}
		case "EXPIRES":
			if len(parts) > 1 {
				fmt.Sscanf(parts[1], "%d", &u.Expires)
			}
		case "GROUP":
			if len(parts) >= 2 {
				group := parts[1]
				isAdmin := 0
				if len(parts) > 2 {
					fmt.Sscanf(parts[2], "%d", &isAdmin)
				}
				u.Groups[group] = isAdmin
			}
		case "PRIMARY", "PRIMARY_GROUP":
			if len(parts) >= 2 {
				u.PrimaryGroup = parts[1]
			}
		case "IP":
			if len(parts) > 1 {
				u.IPs = append(u.IPs, parts[1])
			}
		case "UPLOADSLOTS":
			if len(parts) > 1 {
				fmt.Sscanf(parts[1], "%d", &u.UploadSlots)
				u.UploadSlotsSet = true
			}
		case "DOWNLOADSLOTS":
			if len(parts) > 1 {
				fmt.Sscanf(parts[1], "%d", &u.DownloadSlots)
				u.DownloadSlotsSet = true
			}
		case "TIMEFRAME":
			if len(parts) > 1 {
				u.TimeframeLine = strings.Join(parts[1:], " ")
			}
		}
	}

	if u.HomeDir == "" {
		u.HomeDir = "/"
	}
	if u.HomeRoot == "" {
		u.HomeRoot = "/site"
	}
	u.CurrentDir = u.HomeDir
	if u.PrimaryGroup != "" {
		if _, ok := u.Groups[u.PrimaryGroup]; !ok {
			u.Groups[u.PrimaryGroup] = 0
		}
	}
	if u.UserLine == "" {
		u.UserLine = "Added by GoFTPd"
	}
	if u.GeneralLine == "" {
		u.GeneralLine = "0,120 -1 0 0"
	}
	if u.LoginsLine == "" {
		u.LoginsLine = "0 0 -1 -1"
	}
	if u.TimeframeLine == "" {
		u.TimeframeLine = "0 0"
	}
	if u.AddedBy == "" {
		u.AddedBy = "goftpd"
	}
	if u.LoginsLine != "" && (!u.UploadSlotsSet || !u.DownloadSlotsSet || (u.UploadSlots == 0 && u.DownloadSlots == 0)) {
		u.deriveSlotsFromLogins()
	}

	// Set GID based on primary group
	if groupMap != nil {
		if u.PrimaryGroup != "" {
			// Use explicitly set primary group
			if gid, ok := groupMap[u.PrimaryGroup]; ok {
				u.GID = gid
			}
		} else if len(u.Groups) > 0 {
			// Fallback: use first group (alphabetically sorted for determinism)
			var groups []string
			for g := range u.Groups {
				groups = append(groups, g)
			}
			// Sort for deterministic order
			sort.Strings(groups)
			if gid, ok := groupMap[groups[0]]; ok {
				u.GID = gid
				u.PrimaryGroup = groups[0]
			}
		}
	}

	return u, nil
}

func (u *User) deriveSlotsFromLogins() {
	fields := strings.Fields(u.LoginsLine)
	if len(fields) < 4 {
		return
	}
	var uploadSlots, downloadSlots int
	fmt.Sscanf(fields[2], "%d", &uploadSlots)
	fmt.Sscanf(fields[3], "%d", &downloadSlots)
	if !u.UploadSlotsSet || u.UploadSlots == 0 {
		u.UploadSlots = uploadSlots
		u.UploadSlotsSet = true
	}
	if !u.DownloadSlotsSet || u.DownloadSlots == 0 {
		u.DownloadSlots = downloadSlots
		u.DownloadSlotsSet = true
	}
}

// Save writes user back to userfile-format file
func (u *User) Save() error {
	// Use exact case
	path := filepath.Join("etc", "users", u.Name)
	if u.HomeRoot == "" {
		u.HomeRoot = "/site"
	}
	if u.HomeDir == "" {
		u.HomeDir = "/"
	}
	if u.Tagline == "" {
		u.Tagline = "No Tagline Set"
	}
	if u.Groups == nil {
		u.Groups = make(map[string]int)
	}
	if u.PrimaryGroup != "" {
		if _, ok := u.Groups[u.PrimaryGroup]; !ok {
			u.Groups[u.PrimaryGroup] = 0
		}
	}
	if u.UserLine == "" {
		u.UserLine = "Added by GoFTPd"
	}
	if u.GeneralLine == "" {
		u.GeneralLine = "0,120 -1 0 0"
	}
	if u.LoginsLine == "" {
		u.LoginsLine = "0 0 -1 -1"
	}
	if u.TimeframeLine == "" {
		u.TimeframeLine = "0 0"
	}
	if u.AddedBy == "" {
		u.AddedBy = "goftpd"
	}
	if u.StatExtras == nil {
		u.StatExtras = make(map[string]string)
	}

	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		os.MkdirAll(dir, 0755)
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	fmt.Fprintf(file, "USER %s\n", u.UserLine)
	fmt.Fprintf(file, "GENERAL %s\n", u.GeneralLine)
	fmt.Fprintf(file, "LOGINS %s\n", u.LoginsLine)
	fmt.Fprintf(file, "TIMEFRAME %s\n", u.TimeframeLine)
	fmt.Fprintf(file, "FLAGS %s\n", u.Flags)
	fmt.Fprintf(file, "TAGLINE %s\n", u.Tagline)
	fmt.Fprintf(file, "HOMEDIR %s\n", u.HomeRoot)
	fmt.Fprintf(file, "DIR %s\n", u.HomeDir)
	fmt.Fprintf(file, "ADDED %d %s\n", u.Added, u.AddedBy)
	fmt.Fprintf(file, "EXPIRES %d\n", u.Expires)
	writeValueLine(file, "CREDITS", u.Credits, u.CreditsExtra)
	writeValueLine(file, "RATIO", int64(u.Ratio), u.RatioExtra)
	fmt.Fprintf(file, "UPLOADSLOTS %d\n", u.UploadSlots)
	fmt.Fprintf(file, "DOWNLOADSLOTS %d\n", u.DownloadSlots)
	writeStatLine(file, "ALLUP", u.AllUp, u.StatExtras["ALLUP"])
	writeStatLine(file, "ALLDN", u.AllDn, u.StatExtras["ALLDN"])
	writeStatLine(file, "WKUP", u.WkUp, u.StatExtras["WKUP"])
	writeStatLine(file, "WKDN", u.WkDn, u.StatExtras["WKDN"])
	writeStatLine(file, "DAYUP", u.DayUp, u.StatExtras["DAYUP"])
	writeStatLine(file, "DAYDN", u.DayDn, u.StatExtras["DAYDN"])
	writeStatLine(file, "MONTHUP", u.MonthUp, u.StatExtras["MONTHUP"])
	writeStatLine(file, "MONTHDN", u.MonthDn, u.StatExtras["MONTHDN"])
	writeNukeLine(file, u.NukeStat, u.StatExtras["NUKE"])
	writeTimeLine(file, u.TimeFields, u.LastLogin)

	if u.PrimaryGroup != "" {
		fmt.Fprintf(file, "PRIMARY_GROUP %s\n", u.PrimaryGroup)
	}

	groups := make([]string, 0, len(u.Groups))
	for group := range u.Groups {
		groups = append(groups, group)
	}
	sort.Strings(groups)
	for _, group := range groups {
		fmt.Fprintf(file, "GROUP %s %d\n", group, u.Groups[group])
	}

	ips := append([]string(nil), u.IPs...)
	sort.Strings(ips)
	for _, ip := range ips {
		fmt.Fprintf(file, "IP %s\n", ip)
	}

	return nil
}

// UpdateStats increments throughput metrics and manages credits
func (u *User) UpdateStats(bytes int64, isUpload bool) {
	u.UpdateStatsWithCredits(bytes, isUpload, true)
}

// UpdateStatsWithCredits increments throughput metrics and optionally applies
// ratio credits. Free sections such as speedtest still count traffic, but do
// not add upload credits or charge download credits.
func (u *User) UpdateStatsWithCredits(bytes int64, isUpload bool, applyCredits bool) {
	if isUpload {
		u.AllUp.Files++
		u.AllUp.Bytes += bytes
		u.WkUp.Files++
		u.WkUp.Bytes += bytes
		u.DayUp.Files++
		u.DayUp.Bytes += bytes
		u.MonthUp.Files++
		u.MonthUp.Bytes += bytes

		if applyCredits && u.Ratio > 0 {
			u.Credits += (bytes * int64(u.Ratio))
		}
	} else {
		u.AllDn.Files++
		u.AllDn.Bytes += bytes
		u.WkDn.Files++
		u.WkDn.Bytes += bytes
		u.DayDn.Files++
		u.DayDn.Bytes += bytes
		u.MonthDn.Files++
		u.MonthDn.Bytes += bytes

		if applyCredits && u.Ratio > 0 {
			u.Credits -= bytes
			if u.Credits < 0 {
				u.Credits = 0
			}
		}
	}
	u.Save()
}

func (u *User) HasFlag(flag string) bool {
	return strings.Contains(u.Flags, flag)
}

func (u *User) IsInGroup(group string) bool {
	if u.PrimaryGroup == group {
		return true
	}
	if u.Groups == nil {
		return false
	}
	_, ok := u.Groups[group]
	return ok
}

func (u *User) IsGroupAdmin(group string) bool {
	if u.Groups == nil {
		return false
	}
	val, ok := u.Groups[group]
	return ok && val == 1
}

func (u *User) IsExpired() bool {
	if u.Expires == 0 {
		return false
	}
	return u.Expires < time.Now().Unix()
}

func writeValueLine(file *os.File, key string, value int64, extra string) {
	if extra != "" {
		fmt.Fprintf(file, "%s %d %s\n", key, value, extra)
		return
	}
	fmt.Fprintf(file, "%s %d\n", key, value)
}

func writeStatLine(file *os.File, key string, stat StatLine, extra string) {
	if extra != "" {
		fmt.Fprintf(file, "%s %d %d %d %s\n", key, stat.Files, stat.Bytes, stat.Meta, extra)
		return
	}
	fmt.Fprintf(file, "%s %d %d %d\n", key, stat.Files, stat.Bytes, stat.Meta)
}

func writeTimeLine(file *os.File, fields []string, lastLogin int64) {
	if len(fields) == 0 {
		fmt.Fprintf(file, "TIME %d %d 0 0\n", 0, lastLogin)
		return
	}
	copied := append([]string(nil), fields...)
	for len(copied) < 2 {
		copied = append(copied, "0")
	}
	copied[1] = fmt.Sprintf("%d", lastLogin)
	fmt.Fprintf(file, "TIME %s\n", strings.Join(copied, " "))
}

func writeNukeLine(file *os.File, stat StatLine, extra string) {
	if extra != "" {
		fmt.Fprintf(file, "NUKE %d %d %d %s\n", stat.Meta, stat.Files, stat.Bytes, extra)
		return
	}
	fmt.Fprintf(file, "NUKE %d %d %d\n", stat.Meta, stat.Files, stat.Bytes)
}

func (u *User) IsDisabled() bool {
	return u.HasFlag("6")
}

func (u *User) IPAllowed(remoteIP string) bool {
	if len(u.IPs) == 0 {
		return false
	}
	for _, mask := range u.IPs {
		hostMask := strings.TrimSpace(mask)
		if strings.Contains(hostMask, "@") {
			parts := strings.SplitN(hostMask, "@", 2)
			hostMask = parts[1]
		}
		if hostMask == "*" || hostMask == remoteIP {
			return true
		}
		if ok, _ := pathpkg.Match(hostMask, remoteIP); ok {
			return true
		}
	}
	return false
}

func (u *User) HasEnoughCredits(fileBytes int64) bool {
	if u.Ratio == 0 {
		return true
	}
	return u.Credits >= fileBytes
}

func (u *User) CanDownload(section string, fileBytes int64) bool {
	if !u.HasFlag("3") {
		return false
	}
	return u.HasEnoughCredits(fileBytes)
}
