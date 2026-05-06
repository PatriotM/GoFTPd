package core

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

func listRequestTargetArg(cmd string, args []string) string {
	switch strings.ToUpper(strings.TrimSpace(cmd)) {
	case "LIST", "NLST":
		for _, arg := range args {
			token := strings.TrimSpace(arg)
			if token == "" || strings.HasPrefix(token, "-") {
				continue
			}
			return token
		}
	case "MLSD":
		if len(args) > 0 {
			return strings.TrimSpace(args[0])
		}
	}
	return ""
}

func (s *Session) resolveListTargetPath(cmd string, args []string, bridge MasterBridge) string {
	targetArg := listRequestTargetArg(cmd, args)
	if targetArg == "" {
		switch strings.ToUpper(strings.TrimSpace(s.PretCmd)) {
		case "LIST", "NLST", "MLSD":
			targetArg = strings.TrimSpace(s.PretArg)
		}
	}

	targetPath := s.CurrentDir
	if targetArg != "" {
		if path.IsAbs(targetArg) {
			targetPath = path.Clean(targetArg)
		} else {
			targetPath = path.Clean(path.Join(s.CurrentDir, targetArg))
		}
	}
	if bridge != nil {
		targetPath = bridge.ResolvePath(targetPath)
	}
	return targetPath
}

func (s *Session) hasPreparedDataConnection() bool {
	if s == nil {
		return false
	}
	return s.DataListen != nil || strings.TrimSpace(s.ActiveAddr) != ""
}

func (s *Session) hasPreparedTransferChannel() bool {
	if s == nil {
		return false
	}
	return s.hasPreparedDataConnection() || s.PassthruSlave != nil
}

func (s *Session) masterBridgeOrNil() MasterBridge {
	if s == nil || s.MasterManager == nil {
		return nil
	}
	bridge, _ := s.MasterManager.(MasterBridge)
	return bridge
}

func (s *Session) validateListTargetExists(targetPath string, bridge MasterBridge) error {
	if targetPath == "/" {
		return nil
	}
	if bridge != nil {
		if bridge.FileExists(targetPath) {
			return nil
		}
		return fmt.Errorf("550")
	}
	if s == nil || s.Config == nil {
		return fmt.Errorf("550")
	}
	_, err := os.Lstat(filepath.Join(s.Config.StoragePath, targetPath))
	if err != nil {
		return fmt.Errorf("550")
	}
	return nil
}

func (s *Session) validateListDirectoryTarget(targetPath string, bridge MasterBridge) error {
	if targetPath == "/" {
		return nil
	}
	if bridge != nil {
		entry, found := bridge.GetPathEntry(targetPath)
		if !found {
			return fmt.Errorf("550")
		}
		if !entry.IsDir {
			return fmt.Errorf("504")
		}
		return nil
	}
	if s == nil || s.Config == nil {
		return fmt.Errorf("550")
	}
	info, err := os.Lstat(filepath.Join(s.Config.StoragePath, targetPath))
	if err != nil {
		return fmt.Errorf("550")
	}
	if !info.IsDir() {
		return fmt.Errorf("504")
	}
	return nil
}
