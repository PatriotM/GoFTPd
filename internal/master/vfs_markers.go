package master

import (
	"path"
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
	cleanPath := path.Clean("/" + strings.TrimSpace(filePath))
	if cleanPath == "/" || cleanPath == "." {
		return
	}
	if isDir {
		sm.syncStatusMarkersForDir(cleanPath)
		sm.syncStatusMarkersForDir(path.Dir(cleanPath))
		return
	}
	dirPath := path.Dir(cleanPath)
	sm.syncStatusMarkersForDir(dirPath)
	sm.syncStatusMarkersForDir(path.Dir(dirPath))
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
	if len(childFacts) == 0 {
		childFacts = sm.vfs.GetImmediateChildDirFacts(dirPath)
	}
	progress := sm.GetImmediateReleaseProgress(dirPath)
	if len(progress) == 0 {
		progress = sm.vfs.GetImmediateChildDirProgress(dirPath)
	}

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
