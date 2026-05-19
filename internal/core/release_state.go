package core

import (
	"fmt"
	"path"
	"sort"
	"strings"

	"goftpd/internal/zipscript"
)

func releaseStatusForDir(bridge MasterBridge, dirPath string) (ReleaseStatus, bool) {
	if bridge == nil {
		return ReleaseStatus{}, false
	}
	return bridge.GetReleaseStatus(dirPath)
}

func releaseStatusComplete(status ReleaseStatus) bool {
	return status.Total > 0 && status.Present >= status.Total
}

func syncMasterSFVMissingMarkers(cfg *Config, bridge MasterBridge, dirPath string) {
	if cfg == nil || bridge == nil || !zipscript.ShowMissingFilesForDir(cfg.Zipscript, dirPath) {
		return
	}
	status, ok := releaseStatusForDir(bridge, dirPath)
	if !ok || status.Kind != "sfv" || len(status.ExpectedFiles) == 0 {
		return
	}

	missingSet := make(map[string]bool, len(status.MissingFiles))
	for _, name := range status.MissingFiles {
		missingSet[raceCRCKey(name)] = true
	}

	for _, fileName := range status.ExpectedFiles {
		missingPath := path.Join(dirPath, fileName+"-MISSING")
		if missingSet[raceCRCKey(fileName)] {
			if bridge.GetFileSize(missingPath) < 0 {
				_ = bridge.WriteFile(missingPath, []byte{})
			}
			continue
		}
		if bridge.GetFileSize(missingPath) >= 0 {
			_ = bridge.DeleteFile(missingPath)
		}
	}
}

func clearMasterSFVMissingMarker(bridge MasterBridge, dirPath, fileName string) {
	if bridge == nil {
		return
	}
	missingPath := path.Join(dirPath, fileName+"-MISSING")
	if bridge.GetFileSize(missingPath) >= 0 {
		_ = bridge.DeleteFile(missingPath)
	}
}

func createMasterSFVMissingMarker(cfg *Config, bridge MasterBridge, dirPath, fileName string) {
	if cfg == nil || bridge == nil || !zipscript.ShowMissingFilesForDir(cfg.Zipscript, dirPath) {
		return
	}
	missingPath := path.Join(dirPath, fileName+"-MISSING")
	if bridge.GetFileSize(missingPath) < 0 {
		_ = bridge.WriteFile(missingPath, []byte{})
	}
}

func zipRaceCountsFromStatus(status ReleaseStatus) (present int, total int) {
	if status.Kind != "zip" {
		return 0, 0
	}
	return status.Present, status.Total
}

func sfvRaceCountsFromStatus(status ReleaseStatus) (present int, total int) {
	if status.Kind != "sfv" {
		return 0, 0
	}
	return status.Present, status.Total
}

func missingFilesSummary(status ReleaseStatus) string {
	if len(status.MissingFiles) == 0 {
		return ""
	}
	names := append([]string(nil), status.MissingFiles...)
	sort.Strings(names)
	return fmt.Sprintf("%d missing: %s", len(names), strings.Join(names, ", "))
}
