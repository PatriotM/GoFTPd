package core

import (
	"regexp"
	"sort"
	"strings"

	"goftpd/internal/zipscript"
)

var zipDIZTotalRE = regexp.MustCompile(`[][()<>:[:space:]][[:space:]]*[0-9oOxX*]*[[:space:]]*/[[:space:]]*([0-9oOxX]*[0-9oO])[[:space:]]*[][()<>[:space:]]`)

func shouldAnnounceNoRace(cfg *Config, dirPath string, existingNames []string, fileName string) bool {
	if cfg == nil || !cfg.Zipscript.Enabled || !cfg.Zipscript.Race.AnnounceNoRace {
		return false
	}
	if zipscript.IsIgnoredReleaseSubdir(cfg.Zipscript, dirPath) {
		return false
	}
	if zipscript.UsesRace(cfg.Zipscript, dirPath) || zipscript.IsIgnoredType(cfg.Zipscript, fileName) {
		return false
	}
	if strings.HasPrefix(strings.TrimSpace(fileName), ".") {
		return false
	}
	for _, name := range existingNames {
		name = strings.TrimSpace(name)
		if name == "" || strings.HasPrefix(name, ".") || zipscript.IsIgnoredType(cfg.Zipscript, name) {
			continue
		}
		return false
	}
	return true
}

func isZipPayloadName(name string) bool {
	return zipscript.IsZipPayloadName(name)
}

func isZipRecoverableArchiveName(name string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(name)), ".zip")
}

func zipDirRaceStats(bridge MasterBridge, dirPath string, entries []MasterFileEntry, expectedTotal int) ([]VFSRaceUser, int64, int) {
	userMap := make(map[string]*VFSRaceUser)
	totalBytes := int64(0)
	total := 0
	for _, e := range entries {
		if e.IsDir || e.IsSymlink || strings.HasPrefix(strings.TrimSpace(e.Name), ".") || !isZipPayloadName(e.Name) {
			continue
		}
		total++
		totalBytes += e.Size
		if e.XferTime <= 0 {
			continue
		}
		owner := e.Owner
		if owner == "" {
			owner = "unknown"
		}
		group := e.Group
		if group == "" {
			group = "NoGroup"
		}
		us := userMap[owner]
		if us == nil {
			us = &VFSRaceUser{Name: owner, Group: group}
			userMap[owner] = us
		}
		us.Files++
		us.Bytes += e.Size
		fileSpeed := float64(e.Size) / (float64(e.XferTime) / 1000.0)
		us.Speed += fileSpeed
		if fileSpeed > us.PeakSpeed {
			us.PeakSpeed = fileSpeed
		}
		if us.SlowSpeed == 0 || fileSpeed < us.SlowSpeed {
			us.SlowSpeed = fileSpeed
		}
		us.DurationMs += e.XferTime
	}
	users := make([]VFSRaceUser, 0, len(userMap))
	for _, us := range userMap {
		percentBase := total
		if expectedTotal > 0 {
			percentBase = expectedTotal
		}
		if percentBase > 0 {
			us.Percent = (us.Files * 100) / percentBase
		}
		if us.DurationMs > 0 {
			us.Speed = float64(us.Bytes) / (float64(us.DurationMs) / 1000.0)
		}
		users = append(users, *us)
	}
	sort.Slice(users, func(i, j int) bool {
		if users[i].Files != users[j].Files {
			return users[i].Files > users[j].Files
		}
		if users[i].Bytes != users[j].Bytes {
			return users[i].Bytes > users[j].Bytes
		}
		return strings.ToLower(users[i].Name) < strings.ToLower(users[j].Name)
	})
	return users, totalBytes, total
}

func raceGroupsFromUsers(users []VFSRaceUser, totalFiles int) []VFSRaceGroup {
	groupMap := make(map[string]*VFSRaceGroup)
	for _, u := range users {
		group := strings.TrimSpace(u.Group)
		if group == "" {
			group = "NoGroup"
		}
		g := groupMap[group]
		if g == nil {
			g = &VFSRaceGroup{Name: group}
			groupMap[group] = g
		}
		g.Files += u.Files
		g.Bytes += u.Bytes
		g.Speed += u.Speed
	}
	groups := make([]VFSRaceGroup, 0, len(groupMap))
	for _, g := range groupMap {
		if totalFiles > 0 {
			g.Percent = (g.Files * 100) / totalFiles
		}
		groups = append(groups, *g)
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].Bytes != groups[j].Bytes {
			return groups[i].Bytes > groups[j].Bytes
		}
		if groups[i].Files != groups[j].Files {
			return groups[i].Files > groups[j].Files
		}
		return strings.ToLower(groups[i].Name) < strings.ToLower(groups[j].Name)
	})
	return groups
}
