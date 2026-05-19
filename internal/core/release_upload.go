package core

import (
	"fmt"
	"log"
	"path"
	"strings"
	"time"

	"goftpd/internal/zipscript"
)

type releaseUploadPipelineInput struct {
	UploadDir        string
	MediaInfoDir     string
	FilePath         string
	FileName         string
	Checksum         uint32
	TransferredBytes int64
	FileSize         int64
	SpeedMB          float64
	XferMs           int64
	CompletedAtMs    int64
	ExistingNames    []string
}

type releaseUploadPipelineState struct {
	SFVUpload        bool
	SFVEntries       map[string]uint32
	HadAudioInfo     bool
	HadMediaInfo     bool
	AudioFields      map[string]string
	MediaFields      map[string]string
	RaceUsers        []VFSRaceUser
	RaceGroups       []VFSRaceGroup
	RaceTotalBytes   int64
	RaceTotalFiles   int
	RaceDurationMs   int64
	RaceComplete     bool
	EventData        map[string]string
	ShouldAnnounceNR bool
}

func runReleaseUploadPipeline(s *Session, bridge MasterBridge, in releaseUploadPipelineInput) {
	if s == nil || s.Config == nil || bridge == nil {
		return
	}
	if !finalizeReleaseUpload(s, bridge, in) {
		return
	}

	state := buildReleaseUploadPipelineState(s, bridge, in)
	emitReleaseUploadMetadata(s, bridge, in, state)
	emitReleaseUploadEventAndRace(s, bridge, in, state)
}

func finalizeReleaseUpload(s *Session, bridge MasterBridge, in releaseUploadPipelineInput) bool {
	if s == nil || s.Config == nil || bridge == nil {
		return false
	}

	if badZip, err := checkUploadedZipIntegrity(bridge, s.Config, in.UploadDir, in.FilePath, in.FileName); err != nil && s.Config.Debug {
		log.Printf("[MASTER-ZS] zip integrity check skipped for %s: %v", in.FilePath, err)
	} else if badZip {
		return false
	}

	if in.Checksum > 0 && zipscript.ShouldDeleteBadCRCForDir(s.Config.Zipscript, in.UploadDir) && !strings.HasSuffix(strings.ToLower(in.FileName), ".sfv") {
		sfvEntries := bridge.GetSFVData(in.UploadDir)
		if sfvEntries != nil {
			if expectedCRC, exists := cachedExpectedCRC(sfvEntries, in.FileName); exists {
				if expectedCRC != in.Checksum {
					bridge.DeleteFile(in.FilePath)
					_ = bridge.MarkFileMissing(in.FilePath)
					createMasterSFVMissingMarker(s.Config, bridge, in.UploadDir, in.FileName)
					log.Printf("[MASTER-ZS] CRC mismatch for %s: got %08X, expected %08X - deleted",
						in.FileName, in.Checksum, expectedCRC)
					return false
				}
				clearMasterSFVMissingMarker(bridge, in.UploadDir, in.FileName)
			}
		}
	}

	if s.User != nil && in.TransferredBytes > 0 {
		isSpeedtest := isSpeedtestPath(in.FilePath)
		s.User.UpdateStatsWithCredits(in.TransferredBytes, true, !isSpeedtest)
	}

	return true
}

func buildReleaseUploadPipelineState(s *Session, bridge MasterBridge, in releaseUploadPipelineInput) releaseUploadPipelineState {
	state := releaseUploadPipelineState{
		SFVUpload: strings.HasSuffix(strings.ToLower(in.FileName), ".sfv"),
		EventData: map[string]string{},
	}

	if state.SFVUpload {
		if sfvInfo, err := bridge.GetSFVInfo(in.FilePath); err == nil {
			log.Printf("[MASTER-ZS] Parsed SFV %s: %d entries", in.FileName, len(sfvInfo.Entries))
			bridge.CacheSFV(in.UploadDir, in.FileName, sfvInfo)
		}
	}
	state.SFVEntries = bridge.GetSFVData(in.UploadDir)
	if state.SFVEntries != nil {
		syncMasterSFVMissingMarkers(s.Config, bridge, in.UploadDir)
		bridge.SyncStatusMarkersForPath(in.UploadDir, true)
	}

	if err := refreshZipDIZFromArchive(bridge, in.UploadDir, in.FilePath, in.FileName); err != nil && s.Config.Debug {
		log.Printf("[MASTER-ZS] zip diz refresh skipped for %s: %v", in.FilePath, err)
	}

	state.HadAudioInfo = zipscript.AudioInfoLooksUsable(bridge.GetDirMediaInfo(in.UploadDir))
	state.HadMediaInfo = releaseMediaInfoLooksUsable(bridge.GetDirMediaInfo(in.MediaInfoDir))

	audioFields, err := applyAudioZipscriptChecksForDir(s, bridge, in.UploadDir, in.FilePath, in.FileName)
	if err != nil {
		log.Printf("[MASTER-ZS] post-upload audio check failed for %s: %v", in.FilePath, err)
	} else {
		state.AudioFields = cloneStringMap(audioFields)
	}
	state.MediaFields = probeSTORSitebotMediaInfo(s, bridge, in.MediaInfoDir, in.FilePath, in.FileName, state.HadMediaInfo)

	if subdir := zipscript.ReleaseSubdirLabel(s.Config.Zipscript, in.UploadDir); subdir != "" {
		state.EventData["release_subdir"] = subdir
		state.EventData["release_name"] = path.Base(path.Dir(in.UploadDir))
		if zipscript.IsIgnoredReleaseSubdir(s.Config.Zipscript, in.UploadDir) || !zipscript.AnnounceReleaseSubdirs(s.Config.Zipscript) {
			state.EventData["skip_release_announce"] = "true"
		}
	}
	if state.SFVUpload && state.SFVEntries != nil {
		state.EventData["t_filecount"] = fmt.Sprintf("%d", len(state.SFVEntries))
		state.EventData["t_file_label"] = zipscript.ExpectedFileLabel(s.Config.Zipscript, in.UploadDir)
	}

	state.RaceUsers, state.RaceGroups, state.RaceTotalBytes, state.RaceTotalFiles, state.RaceDurationMs, state.RaceComplete = computeReleaseRaceSnapshot(s, bridge, in, state.EventData)
	state.ShouldAnnounceNR = shouldAnnounceNoRace(s.Config, in.UploadDir, append([]string(nil), in.ExistingNames...), in.FileName)
	return state
}

func computeReleaseRaceSnapshot(s *Session, bridge MasterBridge, in releaseUploadPipelineInput, data map[string]string) ([]VFSRaceUser, []VFSRaceGroup, int64, int, int64, bool) {
	if s == nil || s.Config == nil || bridge == nil || !zipscript.RaceStatsOnSTORForDir(s.Config.Zipscript, in.UploadDir) {
		return nil, nil, 0, 0, 0, false
	}

	if strings.TrimSpace(in.FileName) != "" {
		bridge.NoteRacePayloadTransferAt(in.UploadDir, in.FileName, in.XferMs, in.CompletedAtMs)
	}

	trackedFile := in.FileName
	if strings.HasSuffix(strings.ToLower(in.FileName), ".sfv") {
		trackedFile = firstTrackedRaceFileName(bridge, in.UploadDir)
	}
	return populateUploadRaceData(bridge, s.Config, in.UploadDir, trackedFile, in.FileSize, data)
}

func emitReleaseUploadMetadata(s *Session, bridge MasterBridge, in releaseUploadPipelineInput, state releaseUploadPipelineState) {
	if state.AudioFields != nil {
		emitSTORSitebotAudioInfo(s, bridge, in.UploadDir, in.FilePath, in.FileName, in.TransferredBytes, in.SpeedMB, cloneStringMap(state.AudioFields), state.HadAudioInfo)
	}
	emitSTORSitebotMediaInfo(s, in.MediaInfoDir, in.FilePath, in.FileName, in.TransferredBytes, in.SpeedMB, state.MediaFields, state.HadMediaInfo)
}

func emitReleaseUploadEventAndRace(s *Session, bridge MasterBridge, in releaseUploadPipelineInput, state releaseUploadPipelineState) {
	userName := ""
	if s.User != nil {
		userName = s.User.Name
	}
	enrichUploadRaceUserData(state.EventData, state.RaceUsers, userName)
	s.emitEvent(EventUpload, in.FilePath, in.FileName, in.TransferredBytes, in.SpeedMB, state.EventData)

	if state.ShouldAnnounceNR && zipscript.RaceStatsOnSTORForDir(s.Config.Zipscript, in.UploadDir) {
		emitRaceEndAfter(s, in.UploadDir, nil, nil, in.FileSize, 1, 0, in.XferMs, 0)
		return
	}

	if useZipRaceMode(bridge, s.Config, in.UploadDir, in.FileName) {
		if state.RaceComplete && state.RaceTotalFiles > 0 {
			emitRaceEndAfter(s, in.UploadDir, state.RaceUsers, state.RaceGroups, state.RaceTotalBytes, state.RaceTotalFiles, state.RaceDurationMs, in.XferMs, zipscript.MediaInfoGraceDelayForDir(s.Config.Zipscript, in.UploadDir, in.FileName))
		}
		return
	}

	if state.SFVEntries == nil || !state.RaceComplete {
		return
	}
	if state.SFVUpload || zipscript.CanTriggerRaceEndForDir(s.Config.Zipscript, in.UploadDir, state.SFVEntries, in.FileName) {
		if err := bridge.SyncReleaseRaceStats(in.UploadDir); err != nil && s.Config.Debug {
			log.Printf("[MASTER-ZS] release race sync failed for %s: %v", in.UploadDir, err)
		}
		if state.AudioFields == nil {
			emitOrPrimeReleaseAudioInfo(s, bridge, in.UploadDir)
		}
		emitRaceEndAfter(s, in.UploadDir, state.RaceUsers, state.RaceGroups, state.RaceTotalBytes, state.RaceTotalFiles, state.RaceDurationMs, in.XferMs, zipscript.MediaInfoGraceDelayForDir(s.Config.Zipscript, in.UploadDir, in.FileName))
	}
}

func queueMasterUploadPostHooks(s *Session, bridge MasterBridge, uploadDir, mediaInfoDir, filePath, fileName string, checksum uint32, transferredBytes, fileSize int64, speedMB float64, xferMs int64, existingNames []string) {
	if s == nil || s.Config == nil || bridge == nil {
		return
	}
	input := releaseUploadPipelineInput{
		UploadDir:        uploadDir,
		MediaInfoDir:     mediaInfoDir,
		FilePath:         filePath,
		FileName:         fileName,
		Checksum:         checksum,
		TransferredBytes: transferredBytes,
		FileSize:         fileSize,
		SpeedMB:          speedMB,
		XferMs:           xferMs,
		CompletedAtMs:    time.Now().UnixMilli(),
		ExistingNames:    append([]string(nil), existingNames...),
	}
	enqueueReleasePostHook(uploadDir, func() {
		runReleaseUploadPipeline(s, bridge, input)
	})
}

func zipscriptExistingNames(bridge MasterBridge, dirPath string) []string {
	return zipscriptExistingNamesFromEntries(bridge.ListDir(dirPath))
}

func zipscriptExistingNamesFromEntries(entries []MasterFileEntry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.Name)
	}
	return out
}

func zipscriptExistingDirNames(bridge MasterBridge, dirPath string) []string {
	return zipscriptExistingDirNamesFromEntries(bridge.ListDir(dirPath))
}

func zipscriptExistingDirNamesFromEntries(entries []MasterFileEntry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir {
			out = append(out, entry.Name)
		}
	}
	return out
}
