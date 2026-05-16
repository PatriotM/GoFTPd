package master

import (
	"path"
	"path/filepath"
	"sort"
	"strings"

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

func (sm *SlaveManager) syncStatusMarkersForRelease(releasePath string) {
	if sm == nil || sm.vfs == nil {
		return
	}
	cfg := sm.statusMarkerConfig().Zipscript
	if !zipscript.IncompleteEnabled(cfg) {
		return
	}
	releasePath = path.Clean("/" + strings.TrimSpace(releasePath))
	if releasePath == "/" || releasePath == "." || zipscript.IsIgnoredReleaseSubdir(cfg, releasePath) {
		return
	}
	if !zipscript.UsesReleaseCheckEntry(cfg, releasePath) {
		return
	}

	parentDir := path.Dir(releasePath)
	relName := path.Base(releasePath)
	snapshot, modTime, exists := sm.vfs.GetReleaseStatusSnapshot(releasePath)
	if !exists {
		sm.deleteStatusMarkersForRelease(cfg, parentDir, relName, releasePath)
		return
	}
	if cached := sm.cachedReleaseSnapshot(releasePath); cached != nil {
		if snapshot == nil {
			copyState := *cached
			snapshot = &copyState
		} else {
			mergeReleaseSnapshot(snapshot, cached)
		}
	}

	if snapshot == nil {
		snapshot = &vfsReleaseSnapshot{}
	}

	release := zipscript.StatusMarkerRelease{
		Name:         relName,
		Path:         releasePath,
		ModTime:      modTime,
		VisibleCount: snapshot.VisibleCount,
		HasSFV:       snapshot.HasSFV,
		HasNFO:       snapshot.HasNFO,
		Present:      snapshot.Present,
		Total:        snapshot.Total,
	}

	desired := make(map[string]string, 3)
	for _, marker := range zipscript.BuildStatusMarkerEntries(cfg, parentDir, []zipscript.StatusMarkerRelease{release}) {
		if marker.Name == "" || marker.LinkTarget == "" {
			continue
		}
		desired[path.Join(parentDir, marker.Name)] = marker.LinkTarget
	}

	for _, markerPath := range statusMarkerCandidatePaths(cfg, parentDir, relName) {
		if _, keep := desired[markerPath]; keep {
			continue
		}
		entry := sm.vfs.GetFile(markerPath)
		if entry == nil || !entry.IsSymlink || path.Clean(entry.LinkTarget) != releasePath {
			continue
		}
		sm.vfs.DeleteFile(markerPath)
	}
	for markerPath, targetPath := range desired {
		entry := sm.vfs.GetFile(markerPath)
		if entry != nil && entry.IsSymlink && path.Clean(entry.LinkTarget) == path.Clean(targetPath) {
			continue
		}
		sm.vfs.AddSymlink(markerPath, targetPath)
	}
}

func (sm *SlaveManager) cachedReleaseSnapshot(releasePath string) *vfsReleaseSnapshot {
	if sm == nil {
		return nil
	}
	cleanPath := filepath.Clean(releasePath)
	sm.releaseStateMu.RLock()
	defer sm.releaseStateMu.RUnlock()
	return cloneReleaseSnapshot(sm.releaseFacts[cleanPath])
}

func mergeReleaseSnapshot(dst, src *vfsReleaseSnapshot) {
	if dst == nil || src == nil {
		return
	}
	if src.VisibleCount > dst.VisibleCount {
		dst.VisibleCount = src.VisibleCount
	}
	dst.HasSFV = dst.HasSFV || src.HasSFV
	dst.HasNFO = dst.HasNFO || src.HasNFO
	if src.Present > dst.Present {
		dst.Present = src.Present
	}
	if src.Total > dst.Total {
		dst.Total = src.Total
	}
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

	entries := sm.vfs.ListDirectory(dirPath)
	childFacts := sm.GetImmediateReleaseChildFacts(dirPath)
	childFacts = mergeReleaseChildFacts(childFacts, sm.vfs.GetImmediateChildDirFacts(dirPath))
	progress := sm.GetImmediateReleaseProgress(dirPath)
	progress = mergeReleaseProgress(progress, sm.vfs.GetImmediateChildDirProgress(dirPath))

	desired := make(map[string]string)
	existingMarkers := make(map[string]string)
	evaluatedTargets := make(map[string]struct{}, len(entries))
	releases := make([]zipscript.StatusMarkerRelease, 0, len(entries))
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
		}
		if !entry.IsDir || entry.IsSymlink || zipscript.IsStatusMarkerName(cfg, name) {
			continue
		}
		releasePath := markerLinkTarget(dirPath, name)
		if !zipscript.UsesReleaseCheckEntry(cfg, releasePath) || zipscript.IsIgnoredReleaseSubdir(cfg, releasePath) {
			continue
		}
		facts, hasFacts := childFacts[releasePath]
		stat, ok := progress[releasePath]
		if !ok && !hasFacts {
			continue
		}
		evaluatedTargets[releasePath] = struct{}{}
		release := zipscript.StatusMarkerRelease{
			Name:    name,
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
			release.HasNFO = facts.HasNFO
			if !release.HasSFV {
				release.HasSFV = facts.HasSFV
			}
		}
		releases = append(releases, release)
	}

	for _, marker := range zipscript.BuildStatusMarkerEntries(cfg, dirPath, releases) {
		if marker.Name == "" || marker.LinkTarget == "" {
			continue
		}
		desired[path.Join(dirPath, marker.Name)] = marker.LinkTarget
	}

	for markerPath := range existingMarkers {
		if _, ok := desired[markerPath]; ok {
			continue
		}
		targetPath := path.Clean(existingMarkers[markerPath])
		targetEntry := sm.vfs.GetFile(targetPath)
		if targetEntry == nil || !targetEntry.IsDir || strings.HasPrefix(path.Base(targetPath), "[NUKED]-") {
			sm.vfs.DeleteFile(markerPath)
			continue
		}
		if _, ok := evaluatedTargets[targetPath]; !ok {
			continue
		}
		sm.vfs.DeleteFile(markerPath)
	}
	for markerPath, targetPath := range desired {
		if existingTarget, ok := existingMarkers[markerPath]; ok && path.Clean(existingTarget) == path.Clean(targetPath) {
			continue
		}
		sm.vfs.AddSymlink(markerPath, targetPath)
	}
}

func (sm *SlaveManager) rebuildAllStatusMarkers() {
	if sm == nil || sm.vfs == nil {
		return
	}
	files := sm.vfs.GetAllFiles()
	if len(files) == 0 {
		return
	}
	dirs := make([]string, 0, len(files))
	for filePath, entry := range files {
		if entry == nil || !entry.IsDir || entry.IsSymlink {
			continue
		}
		dirs = append(dirs, filePath)
	}
	sort.Strings(dirs)
	for _, dirPath := range dirs {
		sm.syncStatusMarkersForDir(dirPath)
	}
}
