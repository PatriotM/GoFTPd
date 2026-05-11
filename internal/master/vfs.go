package master

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"goftpd/internal/core"
)

// VFSFile represents a file or directory in the master's virtual file system.
type VFSFile struct {
	Path         string
	Size         int64
	IsDir        bool
	IsSymlink    bool
	LinkTarget   string
	Mode         uint32
	LastModified int64
	SlaveName    string
	Owner        string
	Group        string
	Seen         bool   // Used to detect and purge deleted ghost files
	XferTime     int64  // transfer time in milliseconds (for speed calc)
	Checksum     uint32 // CRC32 from transfer
}

// VFSDirMeta holds per-directory metadata cached on the VFS (like drftpd's pluginMetaData).
// Stored separately from file entries.
type VFSDirMeta struct {
	SFVEntries map[string]uint32 // filename -> CRC32 from parsed SFV
	SFVName    string            // name of the .sfv file
}

type VFSRaceCache struct {
	Users      []RaceUserStat
	Groups     []RaceGroupStat
	TotalBytes int64
	Present    int
	Total      int
}

type VFSSearchResult struct {
	Path    string
	Files   int
	Bytes   int64
	ModTime int64
}

type vfsSnapshot struct {
	Files   map[string]*VFSFile
	DirMeta map[string]*VFSDirMeta
}

type vfsReleaseSnapshot struct {
	VisibleCount int
	HasSFV       bool
	HasNFO       bool
	Present      int
	Total        int
}

// VirtualFileSystem maintains the master's view of files across all slaves.
//
//	/ VirtualFileSystemDirectory.
type VirtualFileSystem struct {
	files          map[string]*VFSFile
	children       map[string]map[string]struct{}
	dirMeta        map[string]*VFSDirMeta // dir path -> metadata (SFV cache etc)
	protectedDirs  map[string]bool
	hiddenPaths    map[string]bool
	excludePaths   map[string]bool
	persistVersion uint64
	savedVersion   uint64
	mu             sync.RWMutex
}

// SlaveNames returns the distinct non-empty slave names currently present in the VFS.
func (vfs *VirtualFileSystem) SlaveNames() []string {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()

	seen := make(map[string]struct{})
	for _, file := range vfs.files {
		if file == nil || file.SlaveName == "" {
			continue
		}
		seen[file.SlaveName] = struct{}{}
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func NewVirtualFileSystem() *VirtualFileSystem {
	vfs := &VirtualFileSystem{
		files:         make(map[string]*VFSFile),
		children:      make(map[string]map[string]struct{}),
		dirMeta:       make(map[string]*VFSDirMeta),
		protectedDirs: make(map[string]bool),
		hiddenPaths:   make(map[string]bool),
		excludePaths:  make(map[string]bool),
	}
	vfs.files["/"] = &VFSFile{Path: "/", IsDir: true, Seen: true}
	vfs.children["/"] = make(map[string]struct{})
	return vfs
}

func (vfs *VirtualFileSystem) AddFile(path string, file VFSFile) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	// Normalize path
	path = cleanVFSPath(path)
	file.Path = path
	if file.LinkTarget != "" {
		file.LinkTarget = cleanVFSPath(file.LinkTarget)
	}
	if vfs.protectedDirs == nil {
		vfs.protectedDirs = make(map[string]bool)
	}
	if vfs.hiddenPaths == nil {
		vfs.hiddenPaths = make(map[string]bool)
	}
	if vfs.excludePaths == nil {
		vfs.excludePaths = make(map[string]bool)
	}
	if vfs.isExcludedPathLocked(path) {
		delete(vfs.files, path)
		return
	}
	if vfs.isHiddenPathLocked(path) {
		delete(vfs.files, path)
		return
	}
	if vfs.protectedDirs[path] {
		file.IsDir = true
		file.IsSymlink = false
		file.LinkTarget = ""
		file.Seen = true
		file.SlaveName = ""
	}
	if file.IsSymlink && file.LinkTarget != "" {
		if existing := vfs.files[path]; existing != nil && existing.IsDir {
			file.IsDir = true
		}
		if target := vfs.files[file.LinkTarget]; target != nil && target.IsDir {
			file.IsDir = true
		}
	}
	if existing := vfs.files[path]; existing != nil &&
		!file.IsDir && !file.IsSymlink &&
		!existing.IsDir && !existing.IsSymlink &&
		existing.Size == file.Size &&
		existing.LastModified == file.LastModified &&
		existing.SlaveName == file.SlaveName {
		if file.Checksum == 0 {
			file.Checksum = existing.Checksum
		}
		if file.XferTime == 0 {
			file.XferTime = existing.XferTime
		}
	}

	vfs.files[path] = &file
	vfs.ensureParentDirsLocked(path, file.SlaveName)
	vfs.linkChildLocked(cleanVFSPath(filepath.Dir(path)), path)
	if file.IsDir {
		vfs.ensureChildrenBucketLocked(path)
	}
	vfs.touchAncestorsLocked(path, file.LastModified)
	vfs.markPersistDirtyLocked()
}

// UpdateFileVerification refreshes checksum-backed verification data for an
// existing file without replacing the whole entry.
func (vfs *VirtualFileSystem) UpdateFileVerification(path string, checksum uint32) bool {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	path = cleanVFSPath(path)
	file := vfs.files[path]
	if file == nil || file.IsDir {
		return false
	}
	file.Checksum = checksum
	vfs.markPersistDirtyLocked()
	return true
}

func (vfs *VirtualFileSystem) SetHiddenPaths(paths []string) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	vfs.hiddenPaths = make(map[string]bool, len(paths))
	for _, p := range paths {
		p = cleanVFSPath(p)
		if p == "" || p == "." || p == "/" {
			continue
		}
		vfs.hiddenPaths[p] = true
	}
	vfs.pruneHiddenPathsLocked()
}

func (vfs *VirtualFileSystem) SetExcludePaths(paths []string) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	vfs.excludePaths = make(map[string]bool, len(paths))
	for _, p := range paths {
		p = cleanVFSPath(p)
		if p == "" || p == "." || p == "/" {
			continue
		}
		vfs.excludePaths[p] = true
	}
	vfs.pruneExcludedPathsLocked()
}

func (vfs *VirtualFileSystem) AddSymlink(linkPath, targetPath string) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	linkPath = cleanVFSPath(linkPath)
	targetPath = cleanVFSPath(targetPath)
	if existing := vfs.files[linkPath]; existing != nil && existing.IsSymlink && existing.LinkTarget == targetPath {
		existing.Seen = true
		return
	}
	vfs.files[linkPath] = &VFSFile{
		Path:         linkPath,
		IsDir:        true,
		IsSymlink:    true,
		LinkTarget:   targetPath,
		Mode:         0777,
		LastModified: time.Now().Unix(),
		Seen:         true,
	}
	vfs.ensureParentDirsLocked(linkPath, "")
	vfs.linkChildLocked(cleanVFSPath(filepath.Dir(linkPath)), linkPath)
	vfs.ensureChildrenBucketLocked(linkPath)
	vfs.touchAncestorsLocked(linkPath, time.Now().Unix())
	vfs.markPersistDirtyLocked()
}

func (vfs *VirtualFileSystem) Chmod(path string, mode uint32) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	path = cleanVFSPath(path)
	if f := vfs.files[path]; f != nil {
		f.Mode = mode
		f.LastModified = time.Now().Unix()
		vfs.markPersistDirtyLocked()
	}
}

// MarkAllUnseen flags all files for a specific slave as unseen before a remerge.
func (vfs *VirtualFileSystem) MarkAllUnseen(slaveName string) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()
	for path, file := range vfs.files {
		if vfs.protectedDirs[path] {
			file.Seen = true
			file.SlaveName = ""
			continue
		}
		if file.SlaveName == slaveName {
			file.Seen = false
		}
	}
}

// PurgeUnseen removes any files for a specific slave that were not seen during the remerge.
func (vfs *VirtualFileSystem) PurgeUnseen(slaveName string) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()
	changed := false
	for path, file := range vfs.files {
		if vfs.protectedDirs[path] {
			file.Seen = true
			file.SlaveName = ""
			continue
		}
		if file.SlaveName == slaveName && !file.Seen {
			changed = vfs.deletePathLocked(path) || changed
		}
	}
	if changed {
		vfs.markPersistDirtyLocked()
	}
}

// PurgeUnseenChildren removes stale direct children for a remerged directory
// immediately, instead of waiting for the slave's full remerge to complete.
func (vfs *VirtualFileSystem) PurgeUnseenChildren(slaveName, dirPath string) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	dirPath = cleanVFSPath(dirPath)
	childPaths := vfs.children[dirPath]
	if len(childPaths) == 0 {
		return
	}

	changed := false
	for childPath := range childPaths {
		file := vfs.files[childPath]
		if file == nil {
			continue
		}
		if file.SlaveName != slaveName || file.Seen {
			continue
		}
		changed = vfs.deletePathLocked(childPath) || changed
	}
	if changed {
		vfs.markPersistDirtyLocked()
	}
}

func (vfs *VirtualFileSystem) SetProtectedDirs(paths []string) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	vfs.protectedDirs = make(map[string]bool, len(paths)+1)
	vfs.protectedDirs["/"] = true
	for _, p := range paths {
		p = cleanVFSPath(p)
		if p == "" || p == "." {
			continue
		}
		vfs.protectedDirs[p] = true
		f := vfs.files[p]
		if f != nil {
			f.Path = p
			f.IsDir = true
			f.Seen = true
			f.SlaveName = ""
		}
	}
	for p, f := range vfs.files {
		p = cleanVFSPath(p)
		if p == "/" || vfs.protectedDirs[p] {
			continue
		}
		if f == nil || !f.IsDir {
			continue
		}
		if cleanVFSPath(filepath.Dir(p)) != "/" {
			continue
		}
		if strings.TrimSpace(f.SlaveName) != "" {
			continue
		}
		delete(vfs.files, p)
	}
	vfs.rebuildChildrenLocked()
	vfs.markPersistDirtyLocked()
}

func (vfs *VirtualFileSystem) GetFile(path string) *VFSFile {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()
	path = cleanVFSPath(path)
	if vfs.isExcludedPathLocked(path) {
		return nil
	}
	if vfs.isHiddenPathLocked(path) {
		return nil
	}
	return vfs.files[path]
}

func (vfs *VirtualFileSystem) ResolvePath(p string) string {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()

	current := cleanVFSPath(p)
	if current == "/" {
		return current
	}

	parts := strings.Split(strings.TrimPrefix(current, "/"), "/")
	current = "/"
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}
		next := cleanVFSPath(filepath.ToSlash(filepath.Join(current, part)))
		if f := vfs.files[next]; f != nil && f.IsSymlink && strings.TrimSpace(f.LinkTarget) != "" {
			current = cleanVFSPath(f.LinkTarget)
			continue
		}
		current = next
	}
	return current
}

func (vfs *VirtualFileSystem) DeleteFile(path string) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	path = cleanVFSPath(path)
	if path == "/" {
		return
	}
	if !vfs.deletePathLocked(path) {
		return
	}
	parent := cleanVFSPath(filepath.Dir(path))
	vfs.touchAncestorsLocked(parent, time.Now().Unix())
	vfs.markPersistDirtyLocked()
}

func (vfs *VirtualFileSystem) deletePathLocked(path string) bool {
	path = cleanVFSPath(path)
	parent := cleanVFSPath(filepath.Dir(path))
	if strings.HasSuffix(strings.ToLower(filepath.Base(path)), ".sfv") {
		if meta := vfs.dirMeta[parent]; meta != nil {
			meta.SFVName = ""
			meta.SFVEntries = nil
			delete(vfs.dirMeta, parent)
		}
	}
	removed := make([]string, 0, 8)
	if _, ok := vfs.files[path]; ok {
		delete(vfs.files, path)
		removed = append(removed, path)
	}

	prefix := path + "/"
	for k := range vfs.files {
		if strings.HasPrefix(k, prefix) {
			delete(vfs.files, k)
			removed = append(removed, k)
		}
	}
	if len(removed) == 0 {
		return false
	}
	if children := vfs.children[parent]; children != nil {
		delete(children, path)
	}
	for _, removedPath := range removed {
		delete(vfs.children, removedPath)
		if removedPath == path {
			continue
		}
		if children := vfs.children[cleanVFSPath(filepath.Dir(removedPath))]; children != nil {
			delete(children, removedPath)
		}
	}
	for metaPath := range vfs.dirMeta {
		if metaPath == path || strings.HasPrefix(metaPath, prefix) {
			delete(vfs.dirMeta, metaPath)
		}
	}
	return true
}

// ListDirectory returns direct children of a directory.
func (vfs *VirtualFileSystem) ListDirectory(dirPath string) []*VFSFile {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()

	dirPath = cleanVFSPath(dirPath)
	childPaths := vfs.children[dirPath]
	if len(childPaths) == 0 {
		return nil
	}

	results := make([]*VFSFile, 0, len(childPaths))
	for childPath := range childPaths {
		if vfs.isHiddenPathLocked(childPath) {
			continue
		}
		if file := vfs.files[childPath]; file != nil {
			results = append(results, file)
		}
	}

	return results
}

// GetImmediateChildDirFacts returns one-pass metadata about direct child
// directories below parentDir, derived from the in-memory VFS.
func (vfs *VirtualFileSystem) GetImmediateChildDirFacts(parentDir string) map[string]core.ReleaseChildFacts {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()

	parentDir = cleanVFSPath(parentDir)
	childPaths := vfs.children[parentDir]
	if len(childPaths) == 0 {
		return nil
	}

	out := make(map[string]core.ReleaseChildFacts, len(childPaths))
	for childPath := range childPaths {
		if vfs.isHiddenPathLocked(childPath) {
			continue
		}
		child := vfs.files[childPath]
		if child == nil || !child.IsDir || child.IsSymlink {
			continue
		}
		snapshot := vfs.computeReleaseSnapshotLocked(childPath)
		facts := core.ReleaseChildFacts{
			Path:         childPath,
			VisibleCount: snapshot.VisibleCount,
			HasSFV:       snapshot.HasSFV,
			HasNFO:       snapshot.HasNFO,
		}
		out[childPath] = facts
	}

	if len(out) == 0 {
		return nil
	}
	return out
}

// GetImmediateChildDirProgress returns cached live SFV progress for direct
// child release directories below parentDir.
func (vfs *VirtualFileSystem) GetImmediateChildDirProgress(parentDir string) map[string]core.ReleaseProgressStat {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()

	parentDir = cleanVFSPath(parentDir)
	childPaths := vfs.children[parentDir]
	if len(childPaths) == 0 {
		return nil
	}

	out := make(map[string]core.ReleaseProgressStat, len(childPaths))
	for childPath := range childPaths {
		if vfs.isHiddenPathLocked(childPath) {
			continue
		}
		child := vfs.files[childPath]
		if child == nil || !child.IsDir || child.IsSymlink {
			continue
		}
		snapshot := vfs.computeReleaseSnapshotLocked(childPath)
		if snapshot.Total <= 0 && !snapshot.HasSFV {
			continue
		}
		out[childPath] = core.ReleaseProgressStat{
			Path:    childPath,
			Present: snapshot.Present,
			Total:   snapshot.Total,
			HasSFV:  snapshot.HasSFV,
		}
	}

	if len(out) == 0 {
		return nil
	}
	return out
}

// GetVerifiedSFVPresentFiles returns the SFV-tracked filenames that are
// currently present and checksum-valid in dirPath, keyed by normalized base
// filename.
func (vfs *VirtualFileSystem) GetVerifiedSFVPresentFiles(dirPath string) map[string]bool {
	return vfs.GetVerifiedSFVPresentFilesFiltered(dirPath, nil)
}

// GetVerifiedSFVPresentFilesFiltered is like GetVerifiedSFVPresentFiles but
// excludes normalized basenames present in excludeKeys. This is used to avoid
// counting files that still exist in the VFS but are actively uploading.
func (vfs *VirtualFileSystem) GetVerifiedSFVPresentFilesFiltered(dirPath string, excludeKeys map[string]bool) map[string]bool {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()

	return vfs.getVerifiedSFVPresentFilesLocked(dirPath, excludeKeys)
}

func (vfs *VirtualFileSystem) getVerifiedSFVPresentFilesLocked(dirPath string, excludeKeys map[string]bool) map[string]bool {
	dirPath = cleanVFSPath(dirPath)
	meta := vfs.dirMeta[dirPath]
	if meta == nil || len(meta.SFVEntries) == 0 {
		return nil
	}

	presentFiles := make(map[string]*VFSFile)
	for childPath := range vfs.children[dirPath] {
		f := vfs.files[childPath]
		if f == nil || f.IsDir {
			continue
		}
		presentFiles[raceFileKey(filepath.Base(childPath))] = f
	}

	verified := make(map[string]bool, len(meta.SFVEntries))
	for sfvFile, expectedCRC := range meta.SFVEntries {
		key := raceFileKey(sfvFile)
		if excludeKeys[key] {
			continue
		}
		f := presentFiles[key]
		if f == nil {
			continue
		}
		if expectedCRC != 0 && f.Checksum != expectedCRC {
			continue
		}
		verified[key] = true
	}

	if len(verified) == 0 {
		return nil
	}
	return verified
}

func (vfs *VirtualFileSystem) computeReleaseSnapshotLocked(dirPath string) *vfsReleaseSnapshot {
	dirPath = cleanVFSPath(dirPath)
	snapshot := &vfsReleaseSnapshot{}
	for childPath := range vfs.children[dirPath] {
		if vfs.isHiddenPathLocked(childPath) {
			continue
		}
		child := vfs.files[childPath]
		if child == nil {
			continue
		}
		name := strings.TrimSpace(filepath.Base(child.Path))
		if strings.HasPrefix(name, ".") {
			continue
		}
		snapshot.VisibleCount++
		if child.IsDir {
			continue
		}
		lower := strings.ToLower(name)
		if strings.HasSuffix(lower, ".sfv") {
			snapshot.HasSFV = true
		}
		if strings.HasSuffix(lower, ".nfo") {
			snapshot.HasNFO = true
		}
	}
	cache := vfs.computeRaceStateFilteredLocked(dirPath, nil)
	if cache != nil {
		snapshot.Present = cache.Present
		snapshot.Total = cache.Total
		if cache.Total > 0 {
			snapshot.HasSFV = true
		}
	}
	return snapshot
}

// SnapshotReleaseFacts captures lightweight per-release listing facts for all
// visible directories so SlaveManager can persist and reuse them outside VFS.
func (vfs *VirtualFileSystem) SnapshotReleaseFacts() map[string]*vfsReleaseSnapshot {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()

	out := make(map[string]*vfsReleaseSnapshot)
	for dirPath, file := range vfs.files {
		if file == nil || !file.IsDir || file.IsSymlink || vfs.isHiddenPathLocked(dirPath) {
			continue
		}
		snapshot := vfs.computeReleaseSnapshotLocked(dirPath)
		if snapshot == nil {
			continue
		}
		if snapshot.VisibleCount == 0 && !snapshot.HasSFV && !snapshot.HasNFO && snapshot.Present == 0 && snapshot.Total == 0 {
			continue
		}
		copyState := *snapshot
		out[dirPath] = &copyState
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// FileExists checks if a path exists in the VFS
func (vfs *VirtualFileSystem) FileExists(path string) bool {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()
	_, exists := vfs.files[filepath.Clean(path)]
	return exists
}

// GetSlavesForPath returns the names of all slaves that have this file
func (vfs *VirtualFileSystem) GetSlavesForPath(path string) []string {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()

	file := vfs.files[filepath.Clean(path)]
	if file == nil {
		return nil
	}
	return []string{file.SlaveName}
}

// RenameFile renames a file/dir in the VFS
func (vfs *VirtualFileSystem) RenameFile(from, to string) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	from = cleanVFSPath(from)
	to = cleanVFSPath(to)
	fromParent := cleanVFSPath(filepath.Dir(from))
	toParent := cleanVFSPath(filepath.Dir(to))
	fromBase := strings.ToLower(strings.TrimSpace(filepath.Base(from)))
	toBase := strings.ToLower(strings.TrimSpace(filepath.Base(to)))

	file := vfs.files[from]
	if file == nil {
		return
	}

	// Move the file itself
	delete(vfs.files, from)
	file.Path = to
	vfs.files[to] = file

	// Move children if directory
	prefix := from + "/"
	var toMove []struct{ old, new string }
	for k := range vfs.files {
		if strings.HasPrefix(k, prefix) {
			newPath := to + "/" + k[len(prefix):]
			toMove = append(toMove, struct{ old, new string }{k, newPath})
		}
	}
	for _, mv := range toMove {
		f := vfs.files[mv.old]
		delete(vfs.files, mv.old)
		f.Path = mv.new
		vfs.files[mv.new] = f
	}

	metaPrefix := from + "/"
	var metaMove []struct{ old, new string }
	for k := range vfs.dirMeta {
		if k == from || strings.HasPrefix(k, metaPrefix) {
			metaMove = append(metaMove, struct{ old, new string }{k, to + k[len(from):]})
		}
	}
	for _, mv := range metaMove {
		meta := vfs.dirMeta[mv.old]
		delete(vfs.dirMeta, mv.old)
		vfs.dirMeta[mv.new] = meta
	}
	if strings.HasSuffix(fromBase, ".sfv") && (fromParent != toParent || !strings.HasSuffix(toBase, ".sfv")) {
		if meta := vfs.dirMeta[fromParent]; meta != nil {
			meta.SFVName = ""
			meta.SFVEntries = nil
			delete(vfs.dirMeta, fromParent)
		}
	}
	vfs.rebuildChildrenLocked()
	now := time.Now().Unix()
	vfs.touchAncestorsLocked(fromParent, now)
	vfs.touchAncestorsLocked(toParent, now)
	vfs.markPersistDirtyLocked()
}

func (vfs *VirtualFileSystem) RelocateFile(from, to, newSlaveName string) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	from = cleanVFSPath(from)
	to = cleanVFSPath(to)
	fromParent := cleanVFSPath(filepath.Dir(from))
	toParent := cleanVFSPath(filepath.Dir(to))

	file := vfs.files[from]
	if file == nil {
		return
	}

	delete(vfs.files, from)
	file.Path = to
	file.SlaveName = newSlaveName
	vfs.files[to] = file

	prefix := from + "/"
	var toMove []struct{ old, new string }
	for k := range vfs.files {
		if strings.HasPrefix(k, prefix) {
			newPath := to + "/" + k[len(prefix):]
			toMove = append(toMove, struct{ old, new string }{k, newPath})
		}
	}
	for _, mv := range toMove {
		f := vfs.files[mv.old]
		delete(vfs.files, mv.old)
		f.Path = mv.new
		f.SlaveName = newSlaveName
		vfs.files[mv.new] = f
	}

	metaPrefix := from + "/"
	var metaMove []struct{ old, new string }
	for k := range vfs.dirMeta {
		if k == from || strings.HasPrefix(k, metaPrefix) {
			metaMove = append(metaMove, struct{ old, new string }{k, to + k[len(from):]})
		}
	}
	for _, mv := range metaMove {
		meta := vfs.dirMeta[mv.old]
		delete(vfs.dirMeta, mv.old)
		vfs.dirMeta[mv.new] = meta
	}

	vfs.rebuildChildrenLocked()
	now := time.Now().Unix()
	vfs.touchAncestorsLocked(fromParent, now)
	vfs.touchAncestorsLocked(toParent, now)
	vfs.markPersistDirtyLocked()
}

// ClearSlave removes all files belonging to a slave (called when slave goes offline)
func (vfs *VirtualFileSystem) ClearSlave(slaveName string) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	changed := false
	for path, file := range vfs.files {
		if file.SlaveName == slaveName {
			delete(vfs.files, path)
			changed = true
		}
	}
	if changed {
		for dirPath := range vfs.dirMeta {
			if file := vfs.files[dirPath]; file == nil || !file.IsDir {
				delete(vfs.dirMeta, dirPath)
			}
		}
		vfs.rebuildChildrenLocked()
		vfs.markPersistDirtyLocked()
	}
}

// GetAllFiles returns all files in the VFS (for debugging)
func (vfs *VirtualFileSystem) GetAllFiles() map[string]*VFSFile {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()

	result := make(map[string]*VFSFile, len(vfs.files))
	for k, v := range vfs.files {
		result[k] = v
	}
	return result
}

func (vfs *VirtualFileSystem) SearchDirs(query string, limit int) []VFSSearchResult {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return nil
	}
	if limit <= 0 {
		limit = 100
	}

	vfs.mu.RLock()
	defer vfs.mu.RUnlock()

	dirs := make([]*VFSFile, 0)
	for _, f := range vfs.files {
		if f == nil || !f.IsDir || f.Path == "/" {
			continue
		}
		cleanPath := filepath.ToSlash(filepath.Clean(f.Path))
		base := filepath.Base(cleanPath)
		if strings.Contains(strings.ToLower(cleanPath), query) || strings.Contains(strings.ToLower(base), query) {
			dirs = append(dirs, f)
		}
	}

	sort.Slice(dirs, func(i, j int) bool {
		return strings.ToLower(filepath.ToSlash(dirs[i].Path)) < strings.ToLower(filepath.ToSlash(dirs[j].Path))
	})

	now := time.Now().Unix()
	results := make([]VFSSearchResult, 0, minInt(limit, len(dirs)))
	for _, dir := range dirs {
		if len(results) >= limit {
			break
		}
		dirPath := filepath.ToSlash(filepath.Clean(dir.Path))
		prefix := strings.TrimRight(dirPath, "/") + "/"
		res := VFSSearchResult{
			Path:    dirPath,
			ModTime: dir.LastModified,
		}
		for _, f := range vfs.files {
			if f == nil || f.IsDir {
				continue
			}
			filePath := filepath.ToSlash(filepath.Clean(f.Path))
			if !strings.HasPrefix(filePath, prefix) {
				continue
			}
			res.Files++
			res.Bytes += f.Size
			if f.LastModified > res.ModTime {
				res.ModTime = f.LastModified
			}
		}
		if res.ModTime <= 0 {
			res.ModTime = now
		}
		results = append(results, res)
	}
	return results
}

// Count returns the number of entries in the VFS
func (vfs *VirtualFileSystem) Count() int {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()
	return len(vfs.files)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// SaveToDisk persists the VFS to a gob file.
// Called on shutdown and periodically by the slave manager.
func (vfs *VirtualFileSystem) SaveToDisk(filePath string) error {
	vfs.mu.RLock()
	version := vfs.persistVersion
	if version == vfs.savedVersion {
		vfs.mu.RUnlock()
		return nil
	}
	snapshot := vfsSnapshot{
		Files:   make(map[string]*VFSFile, len(vfs.files)),
		DirMeta: make(map[string]*VFSDirMeta, len(vfs.dirMeta)),
	}
	for path, file := range vfs.files {
		if file == nil {
			continue
		}
		copyFile := *file
		snapshot.Files[path] = &copyFile
	}
	for dirPath, meta := range vfs.dirMeta {
		if meta == nil {
			continue
		}
		copyMeta := &VFSDirMeta{}
		if len(meta.SFVEntries) > 0 {
			copyMeta.SFVEntries = make(map[string]uint32, len(meta.SFVEntries))
			for name, crc := range meta.SFVEntries {
				copyMeta.SFVEntries[name] = crc
			}
		}
		copyMeta.SFVName = meta.SFVName
		snapshot.DirMeta[dirPath] = copyMeta
	}
	vfs.mu.RUnlock()

	dir := filepath.Dir(filePath)
	if dir != "" && dir != "." {
		os.MkdirAll(dir, 0755)
	}

	// Write to temp file first, then rename (atomic)
	tmpPath := filePath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create vfs file: %w", err)
	}

	enc := gob.NewEncoder(f)
	if err := enc.Encode(snapshot); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("encode vfs: %w", err)
	}
	f.Close()

	if err := os.Rename(tmpPath, filePath); err != nil {
		return fmt.Errorf("rename vfs file: %w", err)
	}

	vfs.mu.Lock()
	if version > vfs.savedVersion {
		vfs.savedVersion = version
	}
	vfs.mu.Unlock()

	log.Printf("[VFS] Saved %d entries to %s", len(snapshot.Files), filePath)
	return nil
}

// LoadFromDisk loads the VFS from a previously saved gob file.
// Called on master startup before slaves connect.
func (vfs *VirtualFileSystem) LoadFromDisk(filePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[VFS] No saved VFS at %s, starting fresh", filePath)
			return nil
		}
		return fmt.Errorf("open vfs file: %w", err)
	}
	defer f.Close()

	vfs.mu.Lock()
	defer vfs.mu.Unlock()

	dec := gob.NewDecoder(f)
	var snapshot vfsSnapshot
	if err := dec.Decode(&snapshot); err != nil {
		if _, seekErr := f.Seek(0, 0); seekErr != nil {
			return fmt.Errorf("decode vfs: %w", err)
		}
		dec = gob.NewDecoder(f)
		var legacyFiles map[string]*VFSFile
		if legacyErr := dec.Decode(&legacyFiles); legacyErr != nil {
			return fmt.Errorf("decode vfs: %w", err)
		}
		vfs.files = legacyFiles
		vfs.dirMeta = make(map[string]*VFSDirMeta)
	} else {
		vfs.files = snapshot.Files
		if snapshot.DirMeta != nil {
			vfs.dirMeta = snapshot.DirMeta
		} else {
			vfs.dirMeta = make(map[string]*VFSDirMeta)
		}
	}

	// Ensure root exists
	if _, ok := vfs.files["/"]; !ok {
		vfs.files["/"] = &VFSFile{Path: "/", IsDir: true, Seen: true}
	}
	if vfs.dirMeta == nil {
		vfs.dirMeta = make(map[string]*VFSDirMeta)
	}
	if vfs.protectedDirs == nil {
		vfs.protectedDirs = make(map[string]bool)
	}
	if vfs.hiddenPaths == nil {
		vfs.hiddenPaths = make(map[string]bool)
	}
	if vfs.excludePaths == nil {
		vfs.excludePaths = make(map[string]bool)
	}
	vfs.pruneExcludedPathsLocked()
	vfs.pruneHiddenPathsLocked()
	vfs.rebuildChildrenLocked()
	vfs.persistVersion = 0
	vfs.savedVersion = 0

	log.Printf("[VFS] Loaded %d entries from %s", len(vfs.files), filePath)
	return nil
}

func (vfs *VirtualFileSystem) isHiddenPathLocked(p string) bool {
	p = cleanVFSPath(p)
	if p == "/" || p == "" {
		return false
	}
	for hidden := range vfs.hiddenPaths {
		if p == hidden || strings.HasPrefix(p, hidden+"/") {
			return true
		}
	}
	return false
}

func (vfs *VirtualFileSystem) isExcludedPathLocked(p string) bool {
	p = cleanVFSPath(p)
	if p == "/" || p == "" {
		return false
	}
	for excluded := range vfs.excludePaths {
		if p == excluded || strings.HasPrefix(p, excluded+"/") {
			return true
		}
	}
	return false
}

func (vfs *VirtualFileSystem) IsExcludedPath(p string) bool {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()
	return vfs.isExcludedPathLocked(p)
}

func (vfs *VirtualFileSystem) pruneExcludedPathsLocked() {
	changed := false
	for p := range vfs.files {
		if vfs.isExcludedPathLocked(p) {
			delete(vfs.files, p)
			delete(vfs.dirMeta, p)
			changed = true
		}
	}
	if changed {
		vfs.rebuildChildrenLocked()
		vfs.markPersistDirtyLocked()
	}
}

func (vfs *VirtualFileSystem) pruneHiddenPathsLocked() {
	changed := false
	for p := range vfs.files {
		if vfs.isHiddenPathLocked(p) {
			delete(vfs.files, p)
			delete(vfs.dirMeta, p)
			changed = true
		}
	}
	if changed {
		vfs.rebuildChildrenLocked()
		vfs.markPersistDirtyLocked()
	}
}

func cleanVFSPath(p string) string {
	p = filepath.ToSlash(filepath.Clean(strings.TrimSpace(p)))
	if p == "." || p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

// SetSFVData caches parsed SFV entries on a directory (like drftpd's pluginMetaData).
func (vfs *VirtualFileSystem) SetSFVData(dirPath string, sfvName string, entries map[string]uint32) {
	vfs.mu.Lock()
	defer vfs.mu.Unlock()
	dirPath = filepath.Clean(dirPath)
	normalized := make(map[string]uint32, len(entries))
	for name, crc := range entries {
		name = raceFileKey(name)
		if name != "" {
			normalized[name] = crc
		}
	}
	meta := vfs.dirMeta[dirPath]
	if meta == nil {
		meta = &VFSDirMeta{}
		vfs.dirMeta[dirPath] = meta
	}
	meta.SFVEntries = normalized
	meta.SFVName = sfvName
	vfs.markPersistDirtyLocked()
}

// GetSFVData returns cached SFV entries for a directory, or nil if not cached.
func (vfs *VirtualFileSystem) GetSFVData(dirPath string) *VFSDirMeta {
	vfs.mu.RLock()
	defer vfs.mu.RUnlock()
	return vfs.dirMeta[filepath.Clean(dirPath)]
}

// RaceUserStat holds per-user race statistics computed from VFS.
type RaceUserStat struct {
	Name       string
	Group      string
	Files      int
	Bytes      int64
	Speed      float64 // bytes/sec average across this user's files
	PeakSpeed  float64 // bytes/sec of this user's fastest single file
	SlowSpeed  float64 // bytes/sec of this user's slowest single file
	Percent    int
	DurationMs int64 // sum of file durations for this user
}

// RaceGroupStat holds per-group race statistics.
type RaceGroupStat struct {
	Name    string
	Files   int
	Bytes   int64
	Speed   float64
	Percent int
}

// GetRaceStats computes race statistics for a directory from VFS metadata.
// Like drftpd's SFVTools.getSFVFiles + RankUtils.userSort.
func (vfs *VirtualFileSystem) GetRaceStats(dirPath string) (users []RaceUserStat, groups []RaceGroupStat, totalBytes int64, present int, total int) {
	return vfs.GetRaceStatsFiltered(dirPath, nil)
}

// GetRaceStatsFiltered computes race stats directly from VFS state.
func (vfs *VirtualFileSystem) GetRaceStatsFiltered(dirPath string, excludeKeys map[string]bool) (users []RaceUserStat, groups []RaceGroupStat, totalBytes int64, present int, total int) {
	vfs.mu.RLock()
	cache := vfs.computeRaceStateFilteredLocked(filepath.Clean(dirPath), excludeKeys)
	if cache == nil {
		vfs.mu.RUnlock()
		return
	}
	users = append(users, cache.Users...)
	groups = append(groups, cache.Groups...)
	totalBytes = cache.TotalBytes
	present = cache.Present
	total = cache.Total
	vfs.mu.RUnlock()
	return
}

func raceFileKey(name string) string {
	name = strings.TrimSpace(filepath.ToSlash(name))
	name = strings.TrimPrefix(name, "./")
	return strings.ToLower(name)
}

func (vfs *VirtualFileSystem) ensureChildrenBucketLocked(dirPath string) {
	dirPath = cleanVFSPath(dirPath)
	if vfs.children == nil {
		vfs.children = make(map[string]map[string]struct{})
	}
	if _, ok := vfs.children[dirPath]; !ok {
		vfs.children[dirPath] = make(map[string]struct{})
	}
}

func (vfs *VirtualFileSystem) linkChildLocked(parentPath, childPath string) {
	parentPath = cleanVFSPath(parentPath)
	childPath = cleanVFSPath(childPath)
	vfs.ensureChildrenBucketLocked(parentPath)
	vfs.children[parentPath][childPath] = struct{}{}
}

func (vfs *VirtualFileSystem) ensureParentDirsLocked(path string, slaveName string) {
	path = cleanVFSPath(path)
	if _, ok := vfs.files["/"]; !ok {
		vfs.files["/"] = &VFSFile{Path: "/", IsDir: true, Seen: true}
	}
	vfs.ensureChildrenBucketLocked("/")

	dir := cleanVFSPath(filepath.Dir(path))
	for dir != "." && dir != "" {
		if existing, exists := vfs.files[dir]; !exists {
			vfs.files[dir] = &VFSFile{
				Path:      dir,
				IsDir:     true,
				SlaveName: slaveName,
				Seen:      true,
			}
		} else {
			existing.Path = dir
			existing.IsDir = true
			existing.Seen = true
		}
		vfs.ensureChildrenBucketLocked(dir)
		if dir == "/" {
			break
		}
		parent := cleanVFSPath(filepath.Dir(dir))
		vfs.linkChildLocked(parent, dir)
		dir = parent
	}
}

func (vfs *VirtualFileSystem) touchAncestorsLocked(path string, ts int64) {
	if ts <= 0 {
		ts = time.Now().Unix()
	}
	current := cleanVFSPath(path)
	for current != "." && current != "" {
		if f := vfs.files[current]; f != nil && f.IsDir {
			if ts > f.LastModified {
				f.LastModified = ts
			}
		}
		if current == "/" {
			break
		}
		current = cleanVFSPath(filepath.Dir(current))
	}
}

func (vfs *VirtualFileSystem) rebuildChildrenLocked() {
	if vfs.files == nil {
		vfs.files = make(map[string]*VFSFile)
	}
	if _, ok := vfs.files["/"]; !ok {
		vfs.files["/"] = &VFSFile{Path: "/", IsDir: true, Seen: true}
	}

	for path, file := range vfs.files {
		if file == nil {
			delete(vfs.files, path)
			continue
		}
		cleanPath := cleanVFSPath(path)
		if cleanPath != path {
			delete(vfs.files, path)
			if _, exists := vfs.files[cleanPath]; !exists {
				file.Path = cleanPath
				vfs.files[cleanPath] = file
			}
			continue
		}
		file.Path = cleanPath
	}

	paths := make([]string, 0, len(vfs.files))
	for path := range vfs.files {
		paths = append(paths, path)
	}
	sort.Slice(paths, func(i, j int) bool {
		di := strings.Count(paths[i], "/")
		dj := strings.Count(paths[j], "/")
		if di != dj {
			return di < dj
		}
		return paths[i] < paths[j]
	})
	for _, path := range paths {
		file := vfs.files[path]
		if file == nil {
			continue
		}
		vfs.ensureParentDirsLocked(path, file.SlaveName)
	}

	children := make(map[string]map[string]struct{})
	for path, file := range vfs.files {
		if file != nil && file.IsDir {
			children[path] = make(map[string]struct{})
		}
	}
	if _, ok := children["/"]; !ok {
		children["/"] = make(map[string]struct{})
	}
	for path := range vfs.files {
		if path == "/" {
			continue
		}
		parent := cleanVFSPath(filepath.Dir(path))
		if _, ok := children[parent]; !ok {
			children[parent] = make(map[string]struct{})
		}
		children[parent][path] = struct{}{}
	}
	vfs.children = children
}

func (vfs *VirtualFileSystem) computeRaceStateFilteredLocked(dirPath string, excludeKeys map[string]bool) *VFSRaceCache {
	dirPath = cleanVFSPath(dirPath)
	meta := vfs.dirMeta[dirPath]
	if meta == nil || len(meta.SFVEntries) == 0 {
		return nil
	}

	userMap := make(map[string]*RaceUserStat)
	groupMap := make(map[string]*RaceGroupStat)
	presentFiles := make(map[string]*VFSFile)
	for childPath := range vfs.children[dirPath] {
		f := vfs.files[childPath]
		if f == nil || f.IsDir {
			continue
		}
		presentFiles[raceFileKey(filepath.Base(childPath))] = f
	}

	cache := &VFSRaceCache{
		Total: len(meta.SFVEntries),
	}
	for sfvFile, expectedCRC := range meta.SFVEntries {
		key := raceFileKey(sfvFile)
		if excludeKeys[key] {
			continue
		}
		f := presentFiles[key]
		if f == nil {
			continue
		}
		if expectedCRC != 0 && f.Checksum != expectedCRC {
			continue
		}
		cache.Present++
		cache.TotalBytes += f.Size

		if f.XferTime <= 0 {
			continue
		}

		owner := f.Owner
		if owner == "" {
			owner = "unknown"
		}
		group := f.Group
		if group == "" {
			group = "NoGroup"
		}

		us := userMap[owner]
		if us == nil {
			us = &RaceUserStat{Name: owner, Group: group}
			userMap[owner] = us
		}
		us.Files++
		us.Bytes += f.Size
		fileSpeed := float64(f.Size) / (float64(f.XferTime) / 1000.0)
		us.Speed += fileSpeed
		if fileSpeed > us.PeakSpeed {
			us.PeakSpeed = fileSpeed
		}
		if us.SlowSpeed == 0 || fileSpeed < us.SlowSpeed {
			us.SlowSpeed = fileSpeed
		}
		us.DurationMs += f.XferTime

		gs := groupMap[group]
		if gs == nil {
			gs = &RaceGroupStat{Name: group}
			groupMap[group] = gs
		}
		gs.Files++
		gs.Bytes += f.Size
	}

	cache.Users = make([]RaceUserStat, 0, len(userMap))
	for _, us := range userMap {
		if cache.Total > 0 {
			us.Percent = (us.Files * 100) / cache.Total
		}
		if us.DurationMs > 0 {
			us.Speed = float64(us.Bytes) / (float64(us.DurationMs) / 1000.0)
		}
		cache.Users = append(cache.Users, *us)
	}
	sort.Slice(cache.Users, func(i, j int) bool {
		if cache.Users[i].Bytes != cache.Users[j].Bytes {
			return cache.Users[i].Bytes > cache.Users[j].Bytes
		}
		if cache.Users[i].Files != cache.Users[j].Files {
			return cache.Users[i].Files > cache.Users[j].Files
		}
		return strings.ToLower(cache.Users[i].Name) < strings.ToLower(cache.Users[j].Name)
	})

	cache.Groups = make([]RaceGroupStat, 0, len(groupMap))
	for _, gs := range groupMap {
		if cache.Total > 0 {
			gs.Percent = (gs.Files * 100) / cache.Total
		}
		for _, us := range cache.Users {
			if us.Group == gs.Name {
				gs.Speed += us.Speed
			}
		}
		cache.Groups = append(cache.Groups, *gs)
	}
	sort.Slice(cache.Groups, func(i, j int) bool {
		if cache.Groups[i].Bytes != cache.Groups[j].Bytes {
			return cache.Groups[i].Bytes > cache.Groups[j].Bytes
		}
		if cache.Groups[i].Files != cache.Groups[j].Files {
			return cache.Groups[i].Files > cache.Groups[j].Files
		}
		return strings.ToLower(cache.Groups[i].Name) < strings.ToLower(cache.Groups[j].Name)
	})

	return cache
}

func (vfs *VirtualFileSystem) markPersistDirtyLocked() {
	vfs.persistVersion++
}
