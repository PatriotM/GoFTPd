package core

import "strings"

func normalizeTransferMode(arg string) (string, bool) {
	if strings.EqualFold(strings.TrimSpace(arg), "S") {
		return "S", true
	}
	return "", false
}

func normalizeTransferStructure(arg string) (string, bool) {
	if strings.EqualFold(strings.TrimSpace(arg), "F") {
		return "F", true
	}
	return "", false
}

func normalizeTransferType(arg string) (string, bool) {
	switch strings.ToUpper(strings.TrimSpace(arg)) {
	case "A":
		return "A", true
	case "I":
		return "I", true
	default:
		return "", false
	}
}
