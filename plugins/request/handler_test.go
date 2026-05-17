package request

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"testing"

	"goftpd/internal/plugin"
)

func TestRequestCreatesIndexFile(t *testing.T) {
	bridge := newRequestTestBridge()
	p := New()
	p.svc = &plugin.Services{Bridge: bridge}

	ctx := &requestTestCtx{user: "alice", group: "iND", flags: "1"}
	p.HandleSiteCommand(ctx, "REQUEST", []string{"Some.Release-TEST"})

	data, ok := bridge.files["/REQUESTS/.requests"]
	if !ok {
		t.Fatalf("expected /REQUESTS/.requests to be created")
	}
	if !strings.Contains(string(data), "Some.Release-TEST") {
		t.Fatalf("expected request file to contain release, got %q", string(data))
	}
	if !strings.Contains(string(data), "by alice") {
		t.Fatalf("expected FTP requester to be tracked, got %q", string(data))
	}
	if _, ok := bridge.dirs["/REQUESTS/REQ-Some.Release-TEST"]; !ok {
		t.Fatalf("expected request directory to be created")
	}
	if got := bridge.dirs["/REQUESTS/REQ-Some.Release-TEST"].Owner; got != "alice" {
		t.Fatalf("expected request dir owner to be FTP user, got %q", got)
	}
}

func TestFTPRequestEmitsAnnounceEvent(t *testing.T) {
	bridge := newRequestTestBridge()
	p := New()
	var eventType, eventPath string
	var eventData map[string]string
	p.svc = &plugin.Services{
		Bridge: bridge,
		EmitEvent: func(evtType, evtPath, filename, section string, size int64, speed float64, data map[string]string) {
			eventType = evtType
			eventPath = evtPath
			eventData = data
		},
	}

	ctx := &requestTestCtx{user: "alice", group: "iND", flags: "1"}
	p.HandleSiteCommand(ctx, "REQUEST", []string{"Some.Release-TEST"})

	if eventType != "CUSTOM" {
		t.Fatalf("expected CUSTOM event, got %q", eventType)
	}
	if eventPath != "/REQUESTS/REQ-Some.Release-TEST" {
		t.Fatalf("unexpected event path %q", eventPath)
	}
	if eventData["template"] != "REQUESTADD" || eventData["requester"] != "alice" {
		t.Fatalf("unexpected request event data %#v", eventData)
	}
}

func TestProxyRequestDoesNotEmitDuplicateAnnounceEvent(t *testing.T) {
	bridge := newRequestTestBridge()
	p := New()
	emitted := false
	p.svc = &plugin.Services{
		Bridge: bridge,
		EmitEvent: func(string, string, string, string, int64, float64, map[string]string) {
			emitted = true
		},
	}

	bot := &requestTestCtx{user: "goftpd", group: "sitebot", flags: "1"}
	p.HandleSiteCommand(bot, "REQUEST", []string{"-by:ircUser", "Proxy.Request-TEST"})

	if emitted {
		t.Fatalf("did not expect proxied sitebot request to emit a duplicate announce event")
	}
}

func TestReqFillRecoversExistingRequestDirWithoutIndexFile(t *testing.T) {
	bridge := newRequestTestBridge()
	bridge.addDir("/REQUESTS", "GoFTPd", "GoFTPd")
	bridge.addDir("/REQUESTS/REQ-Old.Release-TEST", "alice", "iND")
	bridge.files["/REQUESTS/REQ-Old.Release-TEST/file.rar"] = []byte("data")

	p := New()
	p.svc = &plugin.Services{Bridge: bridge}

	ctx := &requestTestCtx{user: "alice", group: "iND", flags: "1"}
	p.HandleSiteCommand(ctx, "REQFILL", []string{"Old.Release-TEST"})

	if _, ok := bridge.dirs["/REQUESTS/FILLED-Old.Release-TEST"]; !ok {
		t.Fatalf("expected existing request dir to be renamed to filled dir")
	}
	if _, ok := bridge.files["/REQUESTS/.requests"]; !ok {
		t.Fatalf("expected request index file to be recreated")
	}
	stats := string(bridge.files["/REQUESTS/.reqfills"])
	if !strings.Contains(stats, "filled by alice") {
		t.Fatalf("expected fill stats to track filler, got %q", stats)
	}

	p.HandleSiteCommand(ctx, "REQTOP", nil)
	if !ctx.hasReply("alice - 1 fill(s)") {
		t.Fatalf("expected REQTOP to show filler, got %#v", ctx.replies)
	}
}

func TestRequestReportsIndexSaveFailure(t *testing.T) {
	bridge := newRequestTestBridge()
	bridge.writeErr = errors.New("disk full")
	p := New()
	p.svc = &plugin.Services{Bridge: bridge}

	ctx := &requestTestCtx{user: "alice", group: "iND", flags: "1"}
	p.HandleSiteCommand(ctx, "REQUEST", []string{"Broken.Release-TEST"})

	if !ctx.hasReply("451 Failed to save request list") {
		t.Fatalf("expected save failure reply, got %#v", ctx.replies)
	}
}

func TestDuplicateRequestRepairsMissingDirectory(t *testing.T) {
	bridge := newRequestTestBridge()
	bridge.addDir("/REQUESTS", "GoFTPd", "GoFTPd")
	bridge.files["/REQUESTS/.requests"] = []byte("[ 1:] Repair.Release-TEST ~ by alice (gl) at 2026-05-16 02:00\n")
	p := New()
	p.svc = &plugin.Services{Bridge: bridge}

	ctx := &requestTestCtx{user: "alice", group: "iND", flags: "1"}
	p.HandleSiteCommand(ctx, "REQUEST", []string{"Repair.Release-TEST"})

	if _, ok := bridge.dirs["/REQUESTS/REQ-Repair.Release-TEST"]; !ok {
		t.Fatalf("expected missing request directory to be repaired")
	}
	if !ctx.hasReply("has already been requested") {
		t.Fatalf("expected duplicate reply, got %#v", ctx.replies)
	}
}

func TestReqFillProxyTracksProvidedUser(t *testing.T) {
	bridge := newRequestTestBridge()
	p := New()
	p.svc = &plugin.Services{Bridge: bridge}

	requester := &requestTestCtx{user: "alice", group: "iND", flags: "1"}
	p.HandleSiteCommand(requester, "REQUEST", []string{"Proxy.Release-TEST"})
	bridge.files["/REQUESTS/REQ-Proxy.Release-TEST/file.rar"] = []byte("data")

	bot := &requestTestCtx{user: "goftpd", group: "sitebot", flags: "1"}
	p.HandleSiteCommand(bot, "REQFILL", []string{"-by:ircUser", "Proxy.Release-TEST"})

	stats := string(bridge.files["/REQUESTS/.reqfills"])
	if !strings.Contains(stats, "filled by ircUser") {
		t.Fatalf("expected proxy filler to be tracked, got %q", stats)
	}
}

func TestRequestProxyTracksProvidedUser(t *testing.T) {
	bridge := newRequestTestBridge()
	p := New()
	p.svc = &plugin.Services{Bridge: bridge}

	bot := &requestTestCtx{user: "goftpd", group: "sitebot", flags: "1"}
	p.HandleSiteCommand(bot, "REQUEST", []string{"-by:ircUser", "Proxy.Request-TEST"})

	requests := string(bridge.files["/REQUESTS/.requests"])
	if !strings.Contains(requests, "by ircUser") {
		t.Fatalf("expected proxied requester to be tracked, got %q", requests)
	}
	dir := bridge.dirs["/REQUESTS/REQ-Proxy.Request-TEST"]
	if dir.Owner != "ircUser" {
		t.Fatalf("expected request dir owner to be proxied user, got %q", dir.Owner)
	}
}

func TestRequestUsesConfiguredStorageSlave(t *testing.T) {
	bridge := newRequestTestBridge()
	p := New()
	p.svc = &plugin.Services{Bridge: bridge}
	p.storageSlave = "LOCAL"

	ctx := &requestTestCtx{user: "alice", group: "iND", flags: "1"}
	p.HandleSiteCommand(ctx, "REQUEST", []string{"Pinned.Release-TEST"})

	if got := bridge.dirSlaves["/REQUESTS"]; got != "LOCAL" {
		t.Fatalf("expected base dir on LOCAL, got %q", got)
	}
	if got := bridge.dirSlaves["/REQUESTS/REQ-Pinned.Release-TEST"]; got != "LOCAL" {
		t.Fatalf("expected request dir on LOCAL, got %q", got)
	}
	if got := bridge.fileSlaves["/REQUESTS/.requests"]; got != "LOCAL" {
		t.Fatalf("expected request index on LOCAL, got %q", got)
	}
}

type requestTestCtx struct {
	user    string
	group   string
	flags   string
	replies []string
}

func (c *requestTestCtx) Reply(format string, args ...interface{}) {
	c.replies = append(c.replies, fmt.Sprintf(format, args...))
}

func (c *requestTestCtx) UserName() string         { return c.user }
func (c *requestTestCtx) UserFlags() string        { return c.flags }
func (c *requestTestCtx) UserPrimaryGroup() string { return c.group }
func (c *requestTestCtx) UserGroups() []string     { return []string{c.group} }

func (c *requestTestCtx) hasReply(needle string) bool {
	for _, reply := range c.replies {
		if strings.Contains(reply, needle) {
			return true
		}
	}
	return false
}

type requestTestBridge struct {
	dirs       map[string]plugin.FileEntry
	files      map[string][]byte
	dirSlaves  map[string]string
	fileSlaves map[string]string
	writeErr   error
}

func newRequestTestBridge() *requestTestBridge {
	return &requestTestBridge{
		dirs:       map[string]plugin.FileEntry{"/": {Name: "/", IsDir: true}},
		files:      map[string][]byte{},
		dirSlaves:  map[string]string{},
		fileSlaves: map[string]string{},
	}
}

func (b *requestTestBridge) addDir(dirPath, owner, group string) {
	dirPath = cleanAbs(dirPath)
	b.dirs[dirPath] = plugin.FileEntry{
		Name:    path.Base(dirPath),
		IsDir:   true,
		Owner:   owner,
		Group:   group,
		ModTime: 1,
	}
}

func (b *requestTestBridge) PluginListDir(dirPath string) []plugin.FileEntry {
	dirPath = cleanAbs(dirPath)
	var out []plugin.FileEntry
	for childPath, entry := range b.dirs {
		if childPath == "/" {
			continue
		}
		if path.Dir(childPath) == dirPath {
			out = append(out, entry)
		}
	}
	for filePath, data := range b.files {
		if path.Dir(filePath) == dirPath {
			out = append(out, plugin.FileEntry{
				Name: path.Base(filePath),
				Size: int64(len(data)),
			})
		}
	}
	return out
}

func (b *requestTestBridge) MakeDir(dirPath, owner, group string) error {
	dirPath = cleanAbs(dirPath)
	if parent := path.Dir(dirPath); parent != "." && parent != dirPath {
		for parent != "/" && parent != "." {
			if _, ok := b.dirs[parent]; ok {
				break
			}
			b.addDir(parent, owner, group)
			parent = path.Dir(parent)
		}
	}
	b.addDir(dirPath, owner, group)
	return nil
}

func (b *requestTestBridge) MakeDirOnSlave(dirPath, owner, group, slaveName string) error {
	if err := b.MakeDir(dirPath, owner, group); err != nil {
		return err
	}
	b.dirSlaves[cleanAbs(dirPath)] = slaveName
	return nil
}

func (b *requestTestBridge) Symlink(string, string) error                         { return nil }
func (b *requestTestBridge) VFSSymlink(string, string) error                      { return nil }
func (b *requestTestBridge) Chmod(string, uint32) error                           { return nil }
func (b *requestTestBridge) CreateSparseFile(string, int64, string, string) error { return nil }

func (b *requestTestBridge) DeleteFile(target string) error {
	target = cleanAbs(target)
	delete(b.files, target)
	delete(b.dirs, target)
	prefix := strings.TrimRight(target, "/") + "/"
	for filePath := range b.files {
		if strings.HasPrefix(filePath, prefix) {
			delete(b.files, filePath)
		}
	}
	for dirPath := range b.dirs {
		if strings.HasPrefix(dirPath, prefix) {
			delete(b.dirs, dirPath)
		}
	}
	return nil
}

func (b *requestTestBridge) RenameFile(from, toDir, toName string) error {
	from = cleanAbs(from)
	to := cleanAbs(path.Join(toDir, toName))
	entry, ok := b.dirs[from]
	if !ok {
		return fmt.Errorf("path not found: %s", from)
	}
	delete(b.dirs, from)
	entry.Name = path.Base(to)
	b.dirs[to] = entry

	fromPrefix := strings.TrimRight(from, "/") + "/"
	toPrefix := strings.TrimRight(to, "/") + "/"
	for filePath, data := range b.files {
		if strings.HasPrefix(filePath, fromPrefix) {
			rel := strings.TrimPrefix(filePath, fromPrefix)
			b.files[toPrefix+rel] = data
			delete(b.files, filePath)
		}
	}
	return nil
}

func (b *requestTestBridge) RelocatePath(string, string, string) error                { return nil }
func (b *requestTestBridge) RelocatePathToSlave(string, string, string, string) error { return nil }

func (b *requestTestBridge) WriteFile(filePath string, content []byte) error {
	if b.writeErr != nil {
		return b.writeErr
	}
	filePath = cleanAbs(filePath)
	b.files[filePath] = append([]byte(nil), content...)
	return nil
}

func (b *requestTestBridge) WriteFileOnSlave(filePath string, content []byte, slaveName string) error {
	if err := b.WriteFile(filePath, content); err != nil {
		return err
	}
	b.fileSlaves[cleanAbs(filePath)] = slaveName
	return nil
}

func (b *requestTestBridge) ReadFile(filePath string) ([]byte, error) {
	filePath = cleanAbs(filePath)
	data, ok := b.files[filePath]
	if !ok {
		return nil, fmt.Errorf("file not found: %s", filePath)
	}
	return append([]byte(nil), data...), nil
}

func (b *requestTestBridge) ProbeMediaInfo(string, string, int) (map[string]string, error) {
	return nil, nil
}
func (b *requestTestBridge) CacheMediaInfo(string, map[string]string) {}
func (b *requestTestBridge) FileExists(p string) bool {
	p = cleanAbs(p)
	_, fileOK := b.files[p]
	_, dirOK := b.dirs[p]
	return fileOK || dirOK
}
func (b *requestTestBridge) GetFileSize(p string) int64 {
	p = cleanAbs(p)
	if data, ok := b.files[p]; ok {
		return int64(len(data))
	}
	return -1
}
func (b *requestTestBridge) GetSFVData(string) map[string]uint32      { return nil }
func (b *requestTestBridge) GetDirMediaInfo(string) map[string]string { return nil }
func (b *requestTestBridge) PluginGetVFSRaceStats(string) ([]plugin.RaceUser, []plugin.RaceGroup, int64, int, int) {
	return nil, nil, 0, 0, 0
}
