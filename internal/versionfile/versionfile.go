package versionfile

import (
	"os"
	"path/filepath"
	"strings"
)

// Load returns a version string loaded from a plain "version" file near the
// given config path. Search order:
//  1. sibling "version" file next to the config
//  2. nearest ancestor "etc/version" while walking upward
//
// If no usable file is found, fallback is returned trimmed.
func Load(configPath, fallback string) string {
	for _, candidate := range candidatePaths(configPath) {
		data, err := os.ReadFile(candidate)
		if err != nil {
			continue
		}
		if version := firstNonEmptyLine(string(data)); version != "" {
			return version
		}
	}
	return strings.TrimSpace(fallback)
}

func candidatePaths(configPath string) []string {
	dir := filepath.Dir(configPath)
	seen := map[string]struct{}{}
	var out []string
	add := func(p string) {
		p = filepath.Clean(p)
		if _, ok := seen[p]; ok {
			return
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}

	add(filepath.Join(dir, "version"))
	for current := filepath.Dir(dir); current != "" && current != "."; {
		add(filepath.Join(current, "etc", "version"))
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		current = parent
	}
	return out
}

func firstNonEmptyLine(s string) string {
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			return line
		}
	}
	return ""
}
