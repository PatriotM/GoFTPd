package main

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	"goftpd/internal/core"
	"goftpd/internal/master"
)

type vfsMoveRequest struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Slave string `json:"slave,omitempty"`
}

type vfsDeleteRequest struct {
	Path string `json:"path"`
}

func startVFSAPIServer(cfg *core.Config, bridge *master.Bridge) *http.Server {
	if cfg == nil || bridge == nil || !cfg.API.Enabled {
		return nil
	}
	listenAddr := strings.TrimSpace(cfg.API.Listen)
	if listenAddr == "" {
		listenAddr = "127.0.0.1:5580"
	}

	mux := http.NewServeMux()
	api := &vfsAPI{cfg: cfg, bridge: bridge}
	mux.HandleFunc("/api/v1/health", api.handleHealth)
	mux.HandleFunc("/api/v1/vfs/move", api.handleVFSMove)
	mux.HandleFunc("/api/v1/vfs/delete", api.handleVFSDelete)

	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("[API] failed to listen on %s: %v", listenAddr, err)
	}
	srv := &http.Server{
		Addr:              listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[API] VFS sync API stopped: %v", err)
		}
	}()
	log.Printf("[API] VFS sync API listening on %s", listenAddr)
	return srv
}

type vfsAPI struct {
	cfg    *core.Config
	bridge *master.Bridge
}

func (a *vfsAPI) handleHealth(w http.ResponseWriter, r *http.Request) {
	if !a.authorize(w, r) {
		return
	}
	writeAPIJSON(w, http.StatusOK, map[string]interface{}{"ok": true})
}

func (a *vfsAPI) handleVFSMove(w http.ResponseWriter, r *http.Request) {
	if !a.authorize(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req vfsMoveRequest
	if err := decodeAPIJSON(w, r, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}
	from, err := requireAPIPath(req.From, "from")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}
	to, err := requireAPIPath(req.To, "to")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := a.bridge.VFSMoveOnly(from, to, req.Slave); err != nil {
		writeAPIError(w, http.StatusConflict, err.Error())
		return
	}
	writeAPIJSON(w, http.StatusOK, map[string]interface{}{
		"ok":    true,
		"from":  from,
		"to":    to,
		"slave": strings.TrimSpace(req.Slave),
	})
}

func (a *vfsAPI) handleVFSDelete(w http.ResponseWriter, r *http.Request) {
	if !a.authorize(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req vfsDeleteRequest
	if err := decodeAPIJSON(w, r, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}
	filePath, err := requireAPIPath(req.Path, "path")
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := a.bridge.VFSDeleteOnly(filePath); err != nil {
		writeAPIError(w, http.StatusConflict, err.Error())
		return
	}
	writeAPIJSON(w, http.StatusOK, map[string]interface{}{
		"ok":   true,
		"path": filePath,
	})
}

func (a *vfsAPI) authorize(w http.ResponseWriter, r *http.Request) bool {
	if a == nil || a.cfg == nil || !a.cfg.API.Enabled {
		writeAPIError(w, http.StatusNotFound, "api disabled")
		return false
	}
	token := strings.TrimSpace(a.cfg.API.Token)
	if token == "" {
		writeAPIError(w, http.StatusUnauthorized, "api token not configured")
		return false
	}
	got := bearerToken(r.Header.Get("Authorization"))
	if got == "" {
		got = strings.TrimSpace(r.Header.Get("X-GoFTPd-Token"))
	}
	if subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
		writeAPIError(w, http.StatusUnauthorized, "unauthorized")
		return false
	}
	return true
}

func bearerToken(header string) string {
	header = strings.TrimSpace(header)
	if len(header) < 7 || !strings.EqualFold(header[:7], "Bearer ") {
		return ""
	}
	return strings.TrimSpace(header[7:])
}

func decodeAPIJSON(w http.ResponseWriter, r *http.Request, dst interface{}) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid json: %w", err)
	}
	return nil
}

func requireAPIPath(raw, field string) (string, error) {
	p := strings.TrimSpace(strings.ReplaceAll(raw, "\\", "/"))
	if p == "" {
		return "", fmt.Errorf("%s must not be empty", field)
	}
	if !strings.HasPrefix(p, "/") {
		return "", fmt.Errorf("%s must be an absolute VFS path", field)
	}
	p = path.Clean(p)
	if p == "/" {
		return "", fmt.Errorf("%s must not be VFS root", field)
	}
	return p, nil
}

func writeAPIJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeAPIError(w http.ResponseWriter, status int, message string) {
	writeAPIJSON(w, status, map[string]interface{}{
		"ok":    false,
		"error": message,
	})
}
