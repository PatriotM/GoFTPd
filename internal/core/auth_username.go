package core

import (
	"path"
	"strings"
)

func normalizeLoginUsername(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	raw = strings.ReplaceAll(raw, "\\", "/")
	raw = path.Base(path.Clean("/" + raw))
	if raw == "." || raw == "/" {
		return ""
	}
	return raw
}
