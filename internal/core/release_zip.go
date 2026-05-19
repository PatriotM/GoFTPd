package core

import (
	"fmt"
	"hash/crc32"
	"path"
	"sort"
	"strconv"
	"strings"

	"goftpd/internal/zipscript"
)

func zipDirPayloadCount(bridge MasterBridge, dirPath string, entries []MasterFileEntry) int {
	total := 0
	for _, e := range entries {
		if e.IsDir || e.IsSymlink || strings.HasPrefix(strings.TrimSpace(e.Name), ".") || !isZipPayloadName(e.Name) {
			continue
		}
		total++
	}
	return total
}

func zipExpectedPartsFromDIZ(bridge MasterBridge, dirPath string, allowRecover bool) int {
	if bridge == nil {
		return 0
	}
	dizPath := path.Join(dirPath, "file_id.diz")
	if expected, ok := bridge.GetZipExpectedParts(dirPath); ok {
		return expected
	}
	content, err := bridge.ReadFile(dizPath)
	if (err != nil || len(content) == 0) && allowRecover {
		recovered, recoverErr := recoverZipDIZFromDirectory(bridge, dirPath)
		if recoverErr != nil || len(recovered) == 0 {
			bridge.CacheZipExpectedParts(dirPath, 0, 0)
			return 0
		}
		content = recovered
	} else if err != nil || len(content) == 0 {
		bridge.CacheZipExpectedParts(dirPath, 0, 0)
		return 0
	}
	expected := zipExpectedPartsFromDIZContent(content)
	dizChecksum, ok := bridge.GetKnownChecksum(dizPath)
	if !ok && len(content) > 0 {
		dizChecksum = crc32.ChecksumIEEE(content)
	}
	bridge.CacheZipExpectedParts(dirPath, expected, dizChecksum)
	return expected
}

func zipExpectedPartsFromDIZContent(content []byte) int {
	if len(content) == 0 {
		return 0
	}
	match := zipDIZTotalRE.FindSubmatch(content)
	if len(match) < 2 {
		return 0
	}
	raw := strings.TrimSpace(string(match[1]))
	if raw == "" {
		return 0
	}
	raw = strings.NewReplacer("o", "0", "O", "0", "x", "0", "X", "0").Replace(raw)
	total, err := strconv.Atoi(raw)
	if err != nil || total <= 0 {
		return 0
	}
	return total
}

func recoverZipDIZFromDirectory(bridge MasterBridge, dirPath string) ([]byte, error) {
	if bridge == nil {
		return nil, fmt.Errorf("bridge unavailable")
	}
	entries := bridge.ListDir(dirPath)
	var archives []string
	for _, entry := range entries {
		if entry.IsDir {
			continue
		}
		if entry.Size <= 0 || entry.XferTime <= 0 {
			continue
		}
		if isZipRecoverableArchiveName(entry.Name) {
			archivePath := path.Join(dirPath, entry.Name)
			archives = append(archives, archivePath)
		}
	}
	sort.Strings(archives)
	for _, archivePath := range archives {
		content, err := bridge.ReadZipEntry(archivePath, "file_id.diz")
		if err != nil || len(content) == 0 {
			continue
		}
		if writeErr := bridge.WriteFile(path.Join(dirPath, "file_id.diz"), content); writeErr != nil {
			return nil, writeErr
		}
		return content, nil
	}
	return nil, fmt.Errorf("file_id.diz not found in any archive")
}

func shouldBlockZipDIZUpload(cfg *Config, dirPath, fileName string) bool {
	if cfg == nil || !zipscript.UsesZip(cfg.Zipscript, dirPath) {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(fileName), "file_id.diz")
}
