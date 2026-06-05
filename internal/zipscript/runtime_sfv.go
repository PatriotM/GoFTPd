package zipscript

import (
	"path"
	"strings"
)

type SFVRuntimeBridge interface {
	GetSFVData(dirPath string) map[string]uint32
	GetKnownChecksum(filePath string) (uint32, bool)
	ChecksumFile(filePath string) (uint32, error)
	DeleteFile(filePath string) error
	MarkFileMissing(filePath string) error
	SyncPresentFile(filePath string, checksum uint32) error
	GetFileSize(filePath string) int64
	WriteFile(filePath string, data []byte) error
}

func ShouldTreatDownloadAsMissing(cfg Config, bridge SFVRuntimeBridge, filePath string, debugLog func(string, ...any)) bool {
	if bridge == nil {
		return false
	}
	dirPath := path.Dir(filePath)
	expectedCRC, exists := CachedExpectedCRC(bridge.GetSFVData(dirPath), path.Base(filePath))
	if !exists || expectedCRC == 0 {
		return false
	}

	if knownCRC, ok := bridge.GetKnownChecksum(filePath); ok {
		if knownCRC == expectedCRC {
			return false
		}
		actualCRC, err := bridge.ChecksumFile(filePath)
		if err != nil {
			if !IsNotFoundDeleteError(err) {
				if debugLog != nil {
					debugLog("[MASTER-ZS] cached bad CRC live checksum failed for %s: cached=%08X expected=%08X err=%v", filePath, knownCRC, expectedCRC, err)
				}
				return false
			}
		} else if actualCRC == expectedCRC {
			_ = bridge.SyncPresentFile(filePath, actualCRC)
			clearSFVMissingMarker(bridge, filePath)
			return false
		} else {
			knownCRC = actualCRC
		}
		if ShowMissingFilesForDir(cfg, dirPath) {
			missingPath := filePath + "-MISSING"
			if bridge.GetFileSize(missingPath) < 0 {
				_ = bridge.WriteFile(missingPath, []byte{})
			}
		}
		if ShouldDeleteBadCRCForDir(cfg, dirPath) {
			if err := bridge.DeleteFile(filePath); err != nil && debugLog != nil && !IsNotFoundDeleteError(err) {
				debugLog("[MASTER-ZS] cached bad CRC delete failed for %s: %v", filePath, err)
			}
			_ = bridge.MarkFileMissing(filePath)
		}
		return true
	}

	return false
}

func clearSFVMissingMarker(bridge SFVRuntimeBridge, filePath string) {
	if bridge == nil {
		return
	}
	missingPath := filePath + "-MISSING"
	if bridge.GetFileSize(missingPath) >= 0 {
		_ = bridge.DeleteFile(missingPath)
	}
}

func IsNotFoundDeleteError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not found") || strings.Contains(msg, "no such file or directory")
}
