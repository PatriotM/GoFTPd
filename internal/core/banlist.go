package core

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const userBanFile = "etc/banlist.txt"

var userBanMu sync.RWMutex

type UserBanEntry struct {
	User   string
	Added  int64
	By     string
	Reason string
}

func LoadUserBanEntries() ([]UserBanEntry, error) {
	userBanMu.RLock()
	defer userBanMu.RUnlock()
	file, err := os.Open(userBanFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()

	var entries []UserBanEntry
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "|", 4)
		entry := UserBanEntry{User: strings.TrimSpace(parts[0])}
		if len(parts) > 1 {
			fmt.Sscanf(strings.TrimSpace(parts[1]), "%d", &entry.Added)
		}
		if len(parts) > 2 {
			entry.By = strings.TrimSpace(parts[2])
		}
		if len(parts) > 3 {
			entry.Reason = strings.TrimSpace(parts[3])
		}
		if entry.User != "" {
			entries = append(entries, entry)
		}
	}
	return entries, scanner.Err()
}

func SaveUserBanEntries(entries []UserBanEntry) error {
	userBanMu.Lock()
	defer userBanMu.Unlock()
	if err := os.MkdirAll(filepath.Dir(userBanFile), 0755); err != nil {
		return err
	}
	file, err := os.Create(userBanFile)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, entry := range entries {
		if strings.TrimSpace(entry.User) == "" {
			continue
		}
		fmt.Fprintf(file, "%s|%d|%s|%s\n", entry.User, entry.Added, entry.By, entry.Reason)
	}
	return nil
}

func FindUserBan(username string) (*UserBanEntry, bool) {
	entries, err := LoadUserBanEntries()
	if err != nil {
		return nil, false
	}
	for _, entry := range entries {
		if strings.EqualFold(entry.User, username) {
			copied := entry
			return &copied, true
		}
	}
	return nil, false
}

func AddUserBan(username, by, reason string) error {
	entries, err := LoadUserBanEntries()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if strings.EqualFold(entry.User, username) {
			return nil
		}
	}
	entries = append(entries, UserBanEntry{
		User:   username,
		Added:  time.Now().Unix(),
		By:     by,
		Reason: reason,
	})
	return SaveUserBanEntries(entries)
}

func RemoveUserBan(username string) (bool, error) {
	entries, err := LoadUserBanEntries()
	if err != nil {
		return false, err
	}
	filtered := make([]UserBanEntry, 0, len(entries))
	removed := false
	for _, entry := range entries {
		if strings.EqualFold(entry.User, username) {
			removed = true
			continue
		}
		filtered = append(filtered, entry)
	}
	if !removed {
		return false, nil
	}
	return true, SaveUserBanEntries(filtered)
}
