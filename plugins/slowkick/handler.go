package slowkick

import (
	"fmt"
	"log"
	"path"
	"strings"
	"sync"
	"time"

	"goftpd/internal/plugin"
	"goftpd/internal/user"
)

type Handler struct {
	svc                   *plugin.Services
	enabled               bool
	monitorUploads        bool
	monitorDownloads      bool
	uploadGrace           time.Duration
	downloadGrace         time.Duration
	minUploadSpeedBytes   float64
	minDownloadSpeedBytes float64
	minUsersOnline        int
	excludeUsers          map[string]struct{}
	excludeGroups         map[string]struct{}
	excludePaths          []string
	excludeExtensions     map[string]struct{}
	announceKick          bool
	tempbanAfterKick      bool
	tempbanDuration       time.Duration
	debug                 bool
	configMu              sync.RWMutex
	mu                    sync.Mutex
	tempBans              map[string]time.Time
}

type configSnapshot struct {
	enabled               bool
	monitorUploads        bool
	monitorDownloads      bool
	uploadGrace           time.Duration
	downloadGrace         time.Duration
	minUploadSpeedBytes   float64
	minDownloadSpeedBytes float64
	minUsersOnline        int
	excludeUsers          map[string]struct{}
	excludeGroups         map[string]struct{}
	excludePaths          []string
	excludeExtensions     map[string]struct{}
	announceKick          bool
	tempbanAfterKick      bool
	tempbanDuration       time.Duration
}

func New() *Handler {
	return &Handler{
		enabled:               true,
		monitorUploads:        true,
		monitorDownloads:      true,
		uploadGrace:           5 * time.Second,
		downloadGrace:         5 * time.Second,
		minUploadSpeedBytes:   25 * 1024,
		minDownloadSpeedBytes: 50 * 1024,
		minUsersOnline:        2,
		excludeUsers:          map[string]struct{}{},
		excludeGroups:         map[string]struct{}{},
		excludePaths:          normalizePaths([]string{"/PRE", "/REQUESTS", "/SPEEDTEST"}),
		excludeExtensions:     lowerSet([]string{"sfv"}),
		tempBans:              map[string]time.Time{},
	}
}

func (h *Handler) Name() string { return "slowkick" }

func (h *Handler) Init(svc *plugin.Services, cfg map[string]interface{}) error {
	h.svc = svc
	return h.applyConfig(cfg, true)
}

func (h *Handler) ReloadConfig(cfg map[string]interface{}) error {
	return h.applyConfig(cfg, false)
}

func (h *Handler) applyConfig(cfg map[string]interface{}, initial bool) error {
	enabled := boolConfig(cfg, "enabled", true)
	monitorUploads := boolConfig(cfg, "monitor_uploads", true)
	monitorDownloads := boolConfig(cfg, "monitor_downloads", true)
	uploadGrace := durationSecondsConfig(cfg, "verify_upload_seconds", 5)
	downloadGrace := durationSecondsConfig(cfg, "verify_download_seconds", 5)
	minUploadSpeedBytes := float64(intConfig(cfg["min_upload_speed_kbps"], 25) * 1024)
	minDownloadSpeedBytes := float64(intConfig(cfg["min_download_speed_kbps"], 50) * 1024)
	minUsersOnline := intConfig(cfg["min_users_online"], 2)
	excludeUsers := lowerSet(stringSliceConfig(cfg["exclude_users"]))
	excludeGroups := lowerSet(stringSliceConfig(cfg["exclude_groups"]))
	excludePaths := normalizePaths([]string{"/PRE", "/REQUESTS", "/SPEEDTEST"})
	if raw, ok := cfg["exclude_paths"]; ok {
		excludePaths = normalizePaths(stringSliceConfig(raw))
	}
	excludeExtensions := lowerSet([]string{"sfv"})
	if raw, ok := cfg["exclude_extensions"]; ok {
		excludeExtensions = lowerSet(normalizeExtensions(stringSliceConfig(raw)))
	}
	announceKick := boolConfig(cfg, "announce_kick", true)
	tempbanAfterKick := boolConfig(cfg, "tempban_after_kick", true)
	tempbanDuration := durationSecondsConfig(cfg, "tempban_seconds", 15)
	debug := boolConfig(cfg, "debug", h.svc != nil && h.svc.Debug)

	h.configMu.Lock()
	h.enabled = enabled
	h.monitorUploads = monitorUploads
	h.monitorDownloads = monitorDownloads
	h.uploadGrace = uploadGrace
	h.downloadGrace = downloadGrace
	h.minUploadSpeedBytes = minUploadSpeedBytes
	h.minDownloadSpeedBytes = minDownloadSpeedBytes
	h.minUsersOnline = minUsersOnline
	h.excludeUsers = excludeUsers
	h.excludeGroups = excludeGroups
	h.excludePaths = excludePaths
	h.excludeExtensions = excludeExtensions
	h.announceKick = announceKick
	h.tempbanAfterKick = tempbanAfterKick
	h.tempbanDuration = tempbanDuration
	h.debug = debug
	h.configMu.Unlock()

	action := "reloaded"
	if initial {
		action = "initialized"
	}
	h.logf(
		"%s enabled=%v uploads=%v downloads=%v up_min=%.1fKB/s down_min=%.1fKB/s min_users=%d tempban=%v tempban_seconds=%d",
		action,
		enabled,
		monitorUploads,
		monitorDownloads,
		minUploadSpeedBytes/1024.0,
		minDownloadSpeedBytes/1024.0,
		minUsersOnline,
		tempbanAfterKick,
		int(tempbanDuration/time.Second),
	)
	return nil
}

func (h *Handler) snapshotConfig() configSnapshot {
	h.configMu.RLock()
	defer h.configMu.RUnlock()
	return configSnapshot{
		enabled:               h.enabled,
		monitorUploads:        h.monitorUploads,
		monitorDownloads:      h.monitorDownloads,
		uploadGrace:           h.uploadGrace,
		downloadGrace:         h.downloadGrace,
		minUploadSpeedBytes:   h.minUploadSpeedBytes,
		minDownloadSpeedBytes: h.minDownloadSpeedBytes,
		minUsersOnline:        h.minUsersOnline,
		excludeUsers:          h.excludeUsers,
		excludeGroups:         h.excludeGroups,
		excludePaths:          h.excludePaths,
		excludeExtensions:     h.excludeExtensions,
		announceKick:          h.announceKick,
		tempbanAfterKick:      h.tempbanAfterKick,
		tempbanDuration:       h.tempbanDuration,
	}
}

func (h *Handler) OnEvent(evt *plugin.Event) error { return nil }

func (h *Handler) ValidateLogin(u *user.User, remoteIP string) error {
	cfg := h.snapshotConfig()
	if u == nil || !cfg.enabled || !cfg.tempbanAfterKick || cfg.tempbanDuration <= 0 {
		return nil
	}
	if _, excluded := cfg.excludeUsers[strings.ToLower(strings.TrimSpace(u.Name))]; excluded {
		return nil
	}
	if _, excluded := cfg.excludeGroups[strings.ToLower(strings.TrimSpace(u.PrimaryGroup))]; excluded {
		return nil
	}
	if until, ok := h.activeTempBan(u.Name, time.Now()); ok {
		remaining := int(time.Until(until).Seconds())
		if remaining < 1 {
			remaining = 1
		}
		return fmt.Errorf("temporarily banned after slow transfer, retry in %ds", remaining)
	}
	return nil
}

func (h *Handler) TransferSpeedPolicy(username, primaryGroup, transferPath, direction string) (int64, int64, int64, bool) {
	username = strings.TrimSpace(username)
	if username == "" {
		return 0, 0, 0, false
	}
	cfg := h.snapshotConfig()
	if !h.shouldApplyTransferPolicyWithConfig(cfg, username, primaryGroup, transferPath, direction) {
		return 0, 0, 0, false
	}
	switch strings.ToLower(strings.TrimSpace(direction)) {
	case "upload":
		if cfg.minUploadSpeedBytes <= 0 {
			return 0, 0, 0, false
		}
		return int64(cfg.minUploadSpeedBytes), 0, int64(cfg.uploadGrace / time.Second), true
	case "download":
		if cfg.minDownloadSpeedBytes <= 0 {
			return 0, 0, 0, false
		}
		return int64(cfg.minDownloadSpeedBytes), 0, int64(cfg.downloadGrace / time.Second), true
	default:
		return 0, 0, 0, false
	}
}

func (h *Handler) HandleSlowTransfer(username, primaryGroup, transferPath, direction, slaveName string, transferIndex int32, actualSpeedBytes, minSpeedBytes int64) {
	now := time.Now()
	h.pruneExpiredTempBans(now)
	cfg := h.snapshotConfig()
	if !h.shouldApplyTransferPolicyWithConfig(cfg, username, primaryGroup, transferPath, direction) {
		return
	}
	if cfg.tempbanAfterKick && cfg.tempbanDuration > 0 {
		h.setTempBan(username, now.Add(cfg.tempbanDuration))
	}
	if cfg.announceKick {
		snap := plugin.ActiveSession{
			User:              username,
			PrimaryGroup:      primaryGroup,
			TransferDirection: direction,
			TransferPath:      transferPath,
			TransferSlaveName: slaveName,
			TransferSlaveIdx:  transferIndex,
		}
		policy := transferPolicy{
			direction:     strings.ToLower(strings.TrimSpace(direction)),
			minSpeedBytes: float64(minSpeedBytes),
		}
		switch policy.direction {
		case "upload":
			policy.kickEvent = "SLOWUPLOADKICK"
		case "download":
			policy.kickEvent = "SLOWDOWNLOADKICK"
		default:
			return
		}
		h.emitSlowEvent(policy.kickEvent, snap, float64(actualSpeedBytes), policy)
		h.logf("kicked %s for slow %s in %s at %.1fKB/s", username, policy.direction, transferPath, float64(actualSpeedBytes)/1024.0)
	}
}

func (h *Handler) Stop() error { return nil }

func (h *Handler) shouldCheckSession(snap plugin.ActiveSession) bool {
	return snap.LoggedIn && h.shouldApplyTransferPolicy(snap.User, snap.PrimaryGroup, snap.TransferPath, snap.TransferDirection)
}

func (h *Handler) shouldApplyTransferPolicy(username, primaryGroup, transferPath, direction string) bool {
	return h.shouldApplyTransferPolicyWithConfig(h.snapshotConfig(), username, primaryGroup, transferPath, direction)
}

func (h *Handler) shouldApplyTransferPolicyWithConfig(cfg configSnapshot, username, primaryGroup, transferPath, direction string) bool {
	if !cfg.enabled {
		return false
	}
	if strings.TrimSpace(username) == "" || strings.TrimSpace(transferPath) == "" {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(direction)) {
	case "upload":
		if !cfg.monitorUploads {
			return false
		}
	case "download":
		if !cfg.monitorDownloads {
			return false
		}
	default:
		return false
	}
	if _, excluded := cfg.excludeUsers[strings.ToLower(strings.TrimSpace(username))]; excluded {
		return false
	}
	if _, excluded := cfg.excludeGroups[strings.ToLower(strings.TrimSpace(primaryGroup))]; excluded {
		return false
	}
	if cfg.minUsersOnline > 0 && h.svc != nil && h.svc.ListActiveSessions != nil {
		loggedIn := 0
		for _, snap := range h.svc.ListActiveSessions() {
			if snap.LoggedIn && strings.TrimSpace(snap.User) != "" {
				loggedIn++
			}
		}
		if loggedIn < cfg.minUsersOnline {
			return false
		}
	}
	cleanPath := strings.ToLower(path.Clean("/" + strings.TrimSpace(transferPath)))
	for _, prefix := range cfg.excludePaths {
		if cleanPath == prefix || strings.HasPrefix(cleanPath, prefix+"/") {
			return false
		}
	}
	ext := strings.TrimPrefix(strings.ToLower(path.Ext(strings.TrimSpace(transferPath))), ".")
	if ext != "" {
		if _, excluded := cfg.excludeExtensions[ext]; excluded {
			return false
		}
	}
	return true
}

type transferPolicy struct {
	direction     string
	minSpeedBytes float64
	kickEvent     string
}

func (h *Handler) setTempBan(username string, until time.Time) {
	username = strings.ToLower(strings.TrimSpace(username))
	if username == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tempBans[username] = until
}

func (h *Handler) activeTempBan(username string, now time.Time) (time.Time, bool) {
	username = strings.ToLower(strings.TrimSpace(username))
	if username == "" {
		return time.Time{}, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	until, ok := h.tempBans[username]
	if !ok {
		return time.Time{}, false
	}
	if !until.After(now) {
		delete(h.tempBans, username)
		return time.Time{}, false
	}
	return until, true
}

func (h *Handler) pruneExpiredTempBans(now time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for username, until := range h.tempBans {
		if !until.After(now) {
			delete(h.tempBans, username)
		}
	}
}

func (h *Handler) logf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	if h.svc != nil && h.svc.Logger != nil {
		h.svc.Logger.Printf("[SLOWKICK] %s", msg)
		return
	}
	log.Printf("[SLOWKICK] %s", msg)
}

func (h *Handler) emitSlowEvent(eventType string, snap plugin.ActiveSession, speed float64, policy transferPolicy) {
	if h.svc == nil || h.svc.EmitEvent == nil {
		return
	}
	cfg := h.snapshotConfig()
	data := map[string]string{
		"username":         strings.TrimSpace(snap.User),
		"group":            strings.TrimSpace(snap.PrimaryGroup),
		"direction":        policy.direction,
		"speed_kbps":       fmt.Sprintf("%.2f", speed/1024.0),
		"min_speed_kbps":   fmt.Sprintf("%.2f", policy.minSpeedBytes/1024.0),
		"min_users_online": fmt.Sprintf("%d", cfg.minUsersOnline),
		"slave_name":       strings.TrimSpace(snap.TransferSlaveName),
		"transfer_index":   fmt.Sprintf("%d", snap.TransferSlaveIdx),
		"session_id":       fmt.Sprintf("%d", snap.ID),
	}
	if cfg.tempbanAfterKick && cfg.tempbanDuration > 0 && strings.Contains(eventType, "KICK") {
		data["tempban_seconds"] = fmt.Sprintf("%d", int(cfg.tempbanDuration/time.Second))
	}
	h.svc.EmitEvent(eventType, snap.TransferPath, path.Base(strings.TrimSpace(snap.TransferPath)), "", 0, speed/(1024.0*1024.0), data)
}

func boolConfig(cfg map[string]interface{}, key string, fallback bool) bool {
	if raw, ok := cfg[key].(bool); ok {
		return raw
	}
	return fallback
}

func intConfig(raw interface{}, fallback int) int {
	switch v := raw.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return fallback
	}
}

func durationSecondsConfig(cfg map[string]interface{}, key string, fallback int) time.Duration {
	return time.Duration(intConfig(cfg[key], fallback)) * time.Second
}

func stringSliceConfig(raw interface{}) []string {
	switch v := raw.(type) {
	case []string:
		return append([]string(nil), v...)
	case []interface{}:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
				out = append(out, strings.TrimSpace(s))
			}
		}
		return out
	default:
		return nil
	}
}

func lowerSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func normalizePaths(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(path.Clean("/" + strings.TrimSpace(value)))
		if value == "" || value == "." {
			continue
		}
		out = append(out, value)
	}
	return out
}

func normalizeExtensions(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(strings.ToLower(value))
		value = strings.TrimPrefix(value, ".")
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}
