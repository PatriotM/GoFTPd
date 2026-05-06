package core

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"goftpd/internal/zipscript"
)

func writeUploadSFVStatus(conn io.Writer, checksum uint32, expectedCRC uint32, hasExpected bool, fileSize int64) {
	if !hasExpected {
		return
	}
	if checksum == expectedCRC && checksum != 0 {
		fmt.Fprintf(conn, "226- checksum match: SLAVE/SFV:%08X\r\n", checksum)
		return
	}
	if checksum == 0 && fileSize > 0 {
		fmt.Fprintf(conn, "226- checksum match: SLAVE/SFV: DISABLED\r\n")
	}
}

func writeUploadNoSFVEntryStatus(conn io.Writer, sfvEntries map[string]uint32, fileName string) {
	if sfvEntries == nil {
		return
	}
	if _, ok := cachedExpectedCRC(sfvEntries, fileName); ok {
		return
	}
	fmt.Fprintf(conn, "226- zipscript - no entry in sfv for file\r\n")
}

func localExpectedCRCForFile(localPath string) (uint32, bool) {
	dirPath := filepath.Dir(localPath)
	baseName := filepath.Base(localPath)
	return cachedExpectedCRC(localSFVEntriesForDir(dirPath), baseName)
}

func localSFVEntriesForDir(dirPath string) map[string]uint32 {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return nil
	}

	sfvNames := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".sfv") {
			continue
		}
		sfvNames = append(sfvNames, entry.Name())
	}
	sort.Strings(sfvNames)

	parsed := map[string]uint32{}
	for _, sfvName := range sfvNames {
		data, err := os.ReadFile(filepath.Join(dirPath, sfvName))
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			entryName, crc, ok := parseLocalSFVEntryLine(line)
			if !ok {
				continue
			}
			parsed[raceCRCKey(entryName)] = crc
		}
	}
	if len(parsed) == 0 {
		return nil
	}
	return parsed
}

func parseLocalSFVEntryLine(line string) (string, uint32, bool) {
	line = strings.TrimRight(line, "\r\n")
	if strings.TrimSpace(line) == "" {
		return "", 0, false
	}
	if strings.HasPrefix(strings.TrimLeftFunc(line, unicode.IsSpace), ";") {
		return "", 0, false
	}
	if len(line) < 9 {
		return "", 0, false
	}

	end := len(line)
	for end > 0 && unicode.IsSpace(rune(line[end-1])) {
		end--
	}
	if end < 8 {
		return "", 0, false
	}

	crcStr := line[end-8 : end]
	crc, err := strconv.ParseUint(crcStr, 16, 32)
	if err != nil {
		return "", 0, false
	}

	sep := end - 8
	if sep <= 0 || !unicode.IsSpace(rune(line[sep-1])) {
		return "", 0, false
	}
	for sep > 0 && unicode.IsSpace(rune(line[sep-1])) {
		sep--
	}

	fileName := strings.TrimSpace(line[:sep])
	if fileName == "" {
		return "", 0, false
	}

	return fileName, uint32(crc), true
}

func localShouldTreatDownloadAsMissing(cfg *Config, filePath, localPath string) bool {
	if cfg == nil {
		return false
	}
	expectedCRC, exists := localExpectedCRCForFile(localPath)
	if !exists || expectedCRC == 0 {
		return false
	}

	checksum, err := localFileCRC(localPath)
	if err != nil {
		return false
	}
	if checksum == expectedCRC {
		return false
	}

	if zipscript.ShouldDeleteBadCRCForDir(cfg.Zipscript, filepath.ToSlash(filepath.Dir(localPath))) {
		_ = os.Remove(localPath)
	}
	_ = filePath
	return true
}

func localFileCRC(localPath string) (uint32, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	hash := crc32.NewIEEE()
	if _, err := io.Copy(hash, file); err != nil {
		return 0, err
	}
	return hash.Sum32(), nil
}
