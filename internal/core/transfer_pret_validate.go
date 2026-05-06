package core

import (
	"fmt"
	"path"
	"strings"
)

func validatePretRequest(s *Session, cmd string, args []string) (preparedCmd, preparedArg string, err error) {
	preparedCmd = strings.ToUpper(strings.TrimSpace(cmd))
	switch preparedCmd {
	case "LIST", "NLST", "MLSD":
		preparedArg = listRequestTargetArg(preparedCmd, args)
		return preparedCmd, preparedArg, nil
	case "RETR":
		if len(args) == 0 || strings.TrimSpace(args[0]) == "" {
			return "", "", fmt.Errorf("PRET RETR requires a target path")
		}
		preparedArg = strings.TrimSpace(args[0])
		if s != nil && s.Config != nil && s.Config.Mode == "master" && s.MasterManager != nil {
			if bridge, ok := s.MasterManager.(MasterBridge); ok {
				target := path.Clean(path.Join(s.CurrentDir, preparedArg))
				if path.IsAbs(preparedArg) {
					target = path.Clean(preparedArg)
				}
				target = bridge.ResolvePath(target)
				if bridge.GetFileSize(target) < 0 {
					return "", "", fmt.Errorf("PRET RETR target not found")
				}
			}
		}
		return preparedCmd, preparedArg, nil
	case "STOR":
		if len(args) == 0 || strings.TrimSpace(args[0]) == "" {
			return "", "", fmt.Errorf("PRET STOR requires a target path")
		}
		preparedArg = strings.TrimSpace(args[0])
		if path.Base(preparedArg) == "." || path.Base(preparedArg) == "/" || strings.TrimSpace(path.Base(preparedArg)) == "" {
			return "", "", fmt.Errorf("PRET STOR target is invalid")
		}
		return preparedCmd, preparedArg, nil
	default:
		return "", "", fmt.Errorf("PRET does not support %s", preparedCmd)
	}
}

func pretSuccessMessage(preparedCmd string) string {
	switch strings.ToUpper(strings.TrimSpace(preparedCmd)) {
	case "LIST", "NLST", "MLSD":
		return "OK, planning to use master for upcoming LIST transfer"
	case "RETR":
		return "OK, planning for upcoming download"
	case "STOR":
		return "OK, planning for upcoming upload"
	default:
		return "OK"
	}
}
