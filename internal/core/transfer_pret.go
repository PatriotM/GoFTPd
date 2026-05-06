package core

import "strings"

func requiresPretForPassive(s *Session) bool {
	if s == nil || s.Config == nil {
		return false
	}
	if strings.ToLower(strings.TrimSpace(s.Config.Mode)) != "master" {
		return false
	}
	return s.MasterManager != nil
}

func hasPretForPassive(s *Session) bool {
	if !requiresPretForPassive(s) {
		return true
	}
	return strings.TrimSpace(s.PretCmd) != ""
}
