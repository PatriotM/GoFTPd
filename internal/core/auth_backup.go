package core

import (
	"bytes"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const authStateBackupInterval = 5 * time.Minute

var authStateBackupMu sync.Mutex

// StartAuthStateBackup keeps a daemon-local safety copy of mutable account
// state. It is intentionally fixed and master-only: slaves do not own auth.
func StartAuthStateBackup(cfg *Config) {
	if cfg == nil || !strings.EqualFold(strings.TrimSpace(cfg.Mode), "master") {
		return
	}
	log.Printf("[AUTHBACKUP] automatic auth-state backup enabled [interval=%s]", authStateBackupInterval)
	go func() {
		runAuthStateBackup(cfg)
		ticker := time.NewTicker(authStateBackupInterval)
		defer ticker.Stop()
		for range ticker.C {
			runAuthStateBackup(cfg)
		}
	}()
}

func runAuthStateBackup(cfg *Config) {
	if err := authStateBackupOnce(cfg); err != nil {
		log.Printf("[AUTHBACKUP] backup failed: %v", err)
	}
}

func authStateBackupOnce(cfg *Config) error {
	authStateBackupMu.Lock()
	defer authStateBackupMu.Unlock()

	passwdPath := "etc/passwd"
	if cfg != nil && strings.TrimSpace(cfg.PasswdFile) != "" {
		passwdPath = strings.TrimSpace(cfg.PasswdFile)
	}

	var errs []string
	passwdFileMu.RLock()
	for _, filePath := range []string{passwdPath, "etc/group"} {
		if err := backupRegularFileDash(filePath); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", filePath, err))
		}
	}
	passwdFileMu.RUnlock()

	for _, filePath := range []string{"etc/permissions.yml", "etc/affils.yml", "etc/slave_denylist.txt"} {
		if err := backupRegularFileDash(filePath); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", filePath, err))
		}
	}

	for _, dir := range []string{"etc/users", "etc/groups"} {
		if err := backupDirectoryMirror(dir, dir+"-"); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", dir, err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

func backupRegularFileDash(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if st.IsDir() {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	return backupAuthFileDash(path, data, st.Mode().Perm())
}

func backupDirectoryMirror(src, dst string) error {
	st, err := os.Stat(src)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !st.IsDir() {
		return nil
	}

	parent := filepath.Dir(dst)
	tmp, err := os.MkdirTemp(parent, "."+filepath.Base(dst)+".tmp-*")
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(tmp)
		}
	}()

	copiedFiles, err := copyAuthBackupTree(src, tmp)
	if err != nil {
		return err
	}
	if copiedFiles == 0 {
		return nil
	}
	if err := os.RemoveAll(dst); err != nil {
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func copyAuthBackupTree(src, dst string) (int, error) {
	copiedFiles := 0
	err := filepath.WalkDir(src, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsNotExist(walkErr) {
				return nil
			}
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil || rel == "." {
			return err
		}
		if shouldSkipAuthBackupEntry(entry.Name()) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		target := filepath.Join(dst, rel)
		info, err := entry.Info()
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		if info.Mode().Type() != 0 {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
			return err
		}
		if err := os.WriteFile(target, data, info.Mode().Perm()); err != nil {
			return err
		}
		copiedFiles++
		return nil
	})
	return copiedFiles, err
}

func shouldSkipAuthBackupEntry(name string) bool {
	if name == ".backup" {
		return true
	}
	return strings.Contains(name, ".tmp-")
}
