package master

import (
	"path"
	"strings"

	"goftpd/internal/core"
	"goftpd/internal/zipscript"
)

type statusMarkerConfig struct {
	Zipscript zipscript.Config
}

func (sm *SlaveManager) SetStatusMarkerConfig(cfg zipscript.Config) {
	sm.statusMarkerMu.Lock()
	sm.statusMarkerCfg = statusMarkerConfig{Zipscript: cfg}
	sm.statusMarkerMu.Unlock()
}

func (sm *SlaveManager) statusMarkerConfig() statusMarkerConfig {
	sm.statusMarkerMu.RLock()
	defer sm.statusMarkerMu.RUnlock()
	return sm.statusMarkerCfg
}

func markerLinkTarget(dirPath, relName string) string {
	return path.Clean(path.Join("/", strings.TrimSpace(dirPath), strings.TrimSpace(relName)))
}

func (sm *SlaveManager) SyncStatusMarkersForPath(filePath string, isDir bool) {
	if sm == nil {
		return
	}
	cfg := sm.statusMarkerConfig().Zipscript
	if !zipscript.IncompleteEnabled(cfg) {
		return
	}
	cleanPath := path.Clean("/" + strings.TrimSpace(filePath))
	if cleanPath == "/" || cleanPath == "." {
		return
	}
	if isDir {
		sm.syncStatusMarkersForTouchedDir(cfg, cleanPath)
		return
	}
	sm.syncStatusMarkersForTouchedFile(cfg, cleanPath)
}

func (sm *SlaveManager) syncStatusMarkersForTouchedFile(cfg zipscript.Config, cleanPath string) {
	dirPath := path.Dir(cleanPath)
	if zipscript.UsesReleaseCheckEntry(cfg, dirPath) && !zipscript.IsIgnoredReleaseSubdir(cfg, dirPath) {
		sm.syncStatusMarkersForRelease(dirPath)
		return
	}

	parentDir := path.Dir(dirPath)
	if parentDir == "/" || parentDir == "." {
		return
	}
	if zipscript.IsIgnoredReleaseSubdir(cfg, dirPath) || zipscript.UsesReleaseCheckEntry(cfg, parentDir) {
		sm.syncStatusMarkersForRelease(parentDir)
		sm.syncStatusMarkersForDir(parentDir)
	}
}

func (sm *SlaveManager) syncStatusMarkersForTouchedDir(cfg zipscript.Config, cleanPath string) {
	if zipscript.UsesReleaseCheckEntry(cfg, cleanPath) && !zipscript.IsIgnoredReleaseSubdir(cfg, cleanPath) {
		sm.syncStatusMarkersForRelease(cleanPath)
		sm.syncStatusMarkersForDir(cleanPath)
		return
	}

	parentDir := path.Dir(cleanPath)
	if parentDir == "/" || parentDir == "." {
		sm.syncStatusMarkersForDir(cleanPath)
		return
	}
	if zipscript.IsIgnoredReleaseSubdir(cfg, cleanPath) || zipscript.UsesReleaseCheckEntry(cfg, parentDir) {
		sm.syncStatusMarkersForRelease(parentDir)
		sm.syncStatusMarkersForDir(parentDir)
		return
	}
	sm.syncStatusMarkersForDir(cleanPath)
}

func (sm *SlaveManager) noteTouchedStatusMarkerRelease(cfg zipscript.Config, cleanPath string, isDir bool, touched map[string]struct{}) {
	if touched == nil {
		return
	}
	cleanPath = path.Clean("/" + strings.TrimSpace(cleanPath))
	if cleanPath == "/" || cleanPath == "." {
		return
	}
	if isDir {
		if zipscript.UsesReleaseCheckEntry(cfg, cleanPath) && !zipscript.IsIgnoredReleaseSubdir(cfg, cleanPath) {
			touched[cleanPath] = struct{}{}
			return
		}
		parentDir := path.Dir(cleanPath)
		if parentDir != "/" && parentDir != "." && (zipscript.IsIgnoredReleaseSubdir(cfg, cleanPath) || zipscript.UsesReleaseCheckEntry(cfg, parentDir)) {
			touched[parentDir] = struct{}{}
		}
		return
	}

	dirPath := path.Dir(cleanPath)
	if zipscript.UsesReleaseCheckEntry(cfg, dirPath) && !zipscript.IsIgnoredReleaseSubdir(cfg, dirPath) {
		touched[dirPath] = struct{}{}
		return
	}
	parentDir := path.Dir(dirPath)
	if parentDir != "/" && parentDir != "." && (zipscript.IsIgnoredReleaseSubdir(cfg, dirPath) || zipscript.UsesReleaseCheckEntry(cfg, parentDir)) {
		touched[parentDir] = struct{}{}
	}
}

func (sm *SlaveManager) syncStatusMarkersForRelease(releasePath string) {
	if sm == nil || sm.vfs == nil {
		return
	}
	cfg := sm.statusMarkerConfig().Zipscript
	if !zipscript.IncompleteEnabled(cfg) {
		return
	}
	releasePath = path.Clean("/" + strings.TrimSpace(releasePath))
	if releasePath == "/" || releasePath == "." {
		return
	}

	parentDir := path.Dir(releasePath)
	relName := path.Base(releasePath)
	sm.deleteStatusMarkersForRelease(cfg, parentDir, relName, releasePath)

	if zipscript.IsIgnoredReleaseSubdir(cfg, releasePath) || !zipscript.UsesReleaseCheckEntry(cfg, releasePath) {
		return
	}
	entry := sm.vfs.GetFile(releasePath)
	if entry == nil || !entry.IsDir || entry.IsSymlink {
		return
	}

	release, ok := sm.statusMarkerReleaseForPath(cfg, releasePath, entry)
	if !ok {
		return
	}
	markers := zipscript.BuildStatusMarkerEntries(cfg, parentDir, []zipscript.StatusMarkerRelease{release})
	if len(markers) == 0 {
		return
	}
	for _, marker := range markers {
		if marker.Name == "" || marker.LinkTarget == "" {
			continue
		}
		sm.vfs.AddSymlink(path.Join(parentDir, marker.Name), marker.LinkTarget)
	}
}

func statusMarkerCanUseCachedProgress(cfg zipscript.Config, releasePath string, facts core.ReleaseChildFacts, hasFacts bool, current core.ReleaseProgressStat, hasCurrent bool) bool {
	if hasCurrent && (current.Total > 0 || current.Present > 0 || current.HasSFV) {
		return true
	}
	if !hasFacts {
		return false
	}
	if zipscript.UsesZipEntry(cfg, releasePath) && facts.FileCount > 0 {
		return true
	}
	return false
}

func mergeStatusMarkerProgress(primary, fallback core.ReleaseProgressStat) core.ReleaseProgressStat {
	out := primary
	if fallback.Present > out.Present {
		out.Present = fallback.Present
	}
	if fallback.Total > out.Total {
		out.Total = fallback.Total
	}
	out.HasSFV = out.HasSFV || fallback.HasSFV
	return out
}

func (sm *SlaveManager) statusMarkerReleaseForPath(cfg zipscript.Config, releasePath string, entry *VFSFile) (zipscript.StatusMarkerRelease, bool) {
	releasePath = path.Clean("/" + strings.TrimSpace(releasePath))
	if releasePath == "/" || releasePath == "." || entry == nil {
		return zipscript.StatusMarkerRelease{}, false
	}
	snapshot, _, hasFacts := sm.vfs.GetReleaseStatusSnapshot(releasePath)
	facts := core.ReleaseChildFacts{Path: releasePath}
	stat := core.ReleaseProgressStat{Path: releasePath}
	ok := false
	if hasFacts && snapshot != nil {
		facts.VisibleCount = snapshot.VisibleCount
		facts.FileCount = snapshot.FileCount
		facts.HasSFV = snapshot.HasSFV
		facts.HasNFO = snapshot.HasNFO
		if snapshot.Total > 0 || snapshot.HasSFV {
			stat.Present = snapshot.Present
			stat.Total = snapshot.Total
			stat.HasSFV = snapshot.HasSFV
			ok = true
		}
	}
	if cached, cachedOK := sm.GetImmediateReleaseProgress(path.Dir(releasePath))[releasePath]; cachedOK && statusMarkerCanUseCachedProgress(cfg, releasePath, facts, hasFacts, stat, ok) {
		if ok {
			stat = mergeStatusMarkerProgress(cached, stat)
		} else {
			stat = cached
		}
		ok = true
	}
	if !ok && !hasFacts {
		return zipscript.StatusMarkerRelease{}, false
	}
	release := zipscript.StatusMarkerRelease{
		Name:    path.Base(releasePath),
		Path:    releasePath,
		ModTime: entry.LastModified,
	}
	if ok {
		release.Present = stat.Present
		release.Total = stat.Total
		release.HasSFV = stat.HasSFV
	}
	if hasFacts {
		release.VisibleCount = facts.VisibleCount
		release.FileCount = facts.FileCount
		release.HasNFO = facts.HasNFO
		if !release.HasSFV {
			release.HasSFV = facts.HasSFV
		}
	}
	return release, true
}

func statusMarkerCandidatePaths(cfg zipscript.Config, parentDir, relName string) []string {
	names := make([]string, 0, 4)
	for _, pattern := range []string{
		zipscript.IncompleteIndicator(cfg, ""),
		zipscript.NoSFVIndicator(cfg),
		zipscript.NFOIndicator(cfg),
	} {
		if name := zipscript.StatusMarkerName(pattern, relName); name != "" {
			names = append(names, name)
		}
	}
	if cdPattern := zipscript.CDIndicator(cfg); cdPattern != "" {
		if name := zipscript.StatusMarkerNameForChild(cdPattern, path.Base(parentDir), relName); name != "" {
			names = append(names, name)
		}
	}

	seen := make(map[string]struct{}, len(names))
	out := make([]string, 0, len(names))
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		markerPath := path.Join(parentDir, name)
		if _, ok := seen[markerPath]; ok {
			continue
		}
		seen[markerPath] = struct{}{}
		out = append(out, markerPath)
	}
	return out
}

func (sm *SlaveManager) deleteStatusMarkersForRelease(cfg zipscript.Config, parentDir, relName, releasePath string) {
	for _, markerPath := range statusMarkerCandidatePaths(cfg, parentDir, relName) {
		entry := sm.vfs.GetFile(markerPath)
		if entry == nil || !entry.IsSymlink || path.Clean(entry.LinkTarget) != releasePath {
			continue
		}
		sm.vfs.DeleteFile(markerPath)
	}
}

func (sm *SlaveManager) pruneStaleStatusMarkersForDir(dirPath string) {
	if sm == nil || sm.vfs == nil {
		return
	}
	cfg := sm.statusMarkerConfig().Zipscript
	if !zipscript.IncompleteEnabled(cfg) {
		return
	}
	dirPath = path.Clean("/" + strings.TrimSpace(dirPath))
	if dirPath == "/" || dirPath == "." {
		return
	}
	for _, entry := range sm.vfs.ListDirectory(dirPath) {
		if entry == nil || !entry.IsSymlink {
			continue
		}
		name := path.Base(entry.Path)
		if !zipscript.IsStatusMarkerName(cfg, name) {
			continue
		}
		cleanTarget := path.Clean("/" + strings.TrimSpace(entry.LinkTarget))
		targetEntry := sm.vfs.GetFile(cleanTarget)
		if cleanTarget == "/" || cleanTarget == "." || path.Dir(cleanTarget) != dirPath || targetEntry == nil || !targetEntry.IsDir {
			sm.vfs.DeleteFile(entry.Path)
		}
	}
}

func (sm *SlaveManager) syncStatusMarkersForDir(dirPath string) {
	if sm == nil || sm.vfs == nil {
		return
	}
	cfg := sm.statusMarkerConfig().Zipscript
	if !zipscript.IncompleteEnabled(cfg) {
		return
	}
	dirPath = path.Clean("/" + strings.TrimSpace(dirPath))
	if dirPath == "/" || dirPath == "." {
		return
	}

	existingMarkers := make(map[string]string)
	entries := sm.vfs.ListDirectory(dirPath)
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		name := path.Base(entry.Path)
		if strings.HasPrefix(strings.TrimSpace(name), ".") {
			continue
		}
		if entry.IsSymlink && zipscript.IsStatusMarkerName(cfg, name) {
			existingMarkers[path.Join(dirPath, name)] = strings.TrimSpace(entry.LinkTarget)
			continue
		}
		if entry.IsDir && !entry.IsSymlink {
			sm.syncStatusMarkersForRelease(entry.Path)
		}
	}

	for markerPath, target := range existingMarkers {
		cleanTarget := path.Clean("/" + strings.TrimSpace(target))
		targetEntry := sm.vfs.GetFile(cleanTarget)
		if cleanTarget == "/" || cleanTarget == "." || path.Dir(cleanTarget) != dirPath || targetEntry == nil || !targetEntry.IsDir {
			sm.vfs.DeleteFile(markerPath)
		}
	}
}
