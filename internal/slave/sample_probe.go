package slave

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func probeFastVideoMetadata(fullPath string) (map[string]string, bool, error) {
	switch strings.ToLower(strings.TrimPrefix(filepath.Ext(fullPath), ".")) {
	case "avi":
		fields, err := probeAVIMetadata(fullPath)
		return fields, true, err
	case "mp4", "m4v":
		fields, err := probeMP4Metadata(fullPath)
		return fields, true, err
	case "mkv":
		fields, err := probeMKVMetadata(fullPath)
		return fields, true, err
	case "m2ts", "ts":
		fields, err := probeTSMetadata(fullPath)
		return fields, true, err
	default:
		return nil, false, nil
	}
}

func probeTSMetadata(fullPath string) (map[string]string, error) {
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	packetSize, prefix, err := detectTSPacketLayout(f)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	state := tsProbeState{
		packetSize: packetSize,
		prefixSize: prefix,
		pmtPID:     -1,
		streams:    map[uint16]*tsStreamInfo{},
	}
	reader := bufio.NewReaderSize(f, packetSize*8)
	packet := make([]byte, packetSize)
	for {
		_, err := io.ReadFull(reader, packet)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return nil, err
		}
		if err := parseTSPacket(packet[prefix:], &state); err != nil {
			continue
		}
	}

	fields := map[string]string{}
	video := state.bestStream(tsKindVideo)
	audio := state.bestStream(tsKindAudio)
	subs := state.bestStream(tsKindSubtitle)
	if video != nil {
		if video.codec != "" {
			fields["video_format"] = normalizeVideoCodec(video.codec)
		}
		if video.width > 0 {
			fields["width"] = strconv.Itoa(video.width)
		}
		if video.height > 0 {
			fields["height"] = strconv.Itoa(video.height)
		}
	}
	if audio != nil {
		if audio.codec != "" {
			fields["audio_format"] = normalizeAudioCodec(audio.codec)
		}
		if audio.channels > 0 {
			fields["channels"] = strconv.Itoa(audio.channels)
		}
	}
	if subs != nil && subs.codec != "" {
		fields["subtitle_format"] = normalizeSubtitleCodec(subs.codec)
	}
	if duration := state.bestDurationSeconds(); duration > 0 {
		fields["duration"] = formatFloatSeconds(duration)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("ts: no usable metadata")
	}
	deriveMediaInfoFields(fields)
	return fields, nil
}

type tsStreamKind int

const (
	tsKindUnknown tsStreamKind = iota
	tsKindVideo
	tsKindAudio
	tsKindSubtitle
)

type tsStreamInfo struct {
	pid      uint16
	kind     tsStreamKind
	codec    string
	channels int
	firstPTS int64
	lastPTS  int64
	width    int
	height   int
}

type tsProbeState struct {
	packetSize int
	prefixSize int
	pmtPID     int
	streams    map[uint16]*tsStreamInfo
}

func (s *tsProbeState) bestStream(kind tsStreamKind) *tsStreamInfo {
	for _, stream := range s.streams {
		if stream.kind == kind {
			return stream
		}
	}
	return nil
}

func (s *tsProbeState) bestDurationSeconds() float64 {
	for _, stream := range s.streams {
		if stream.kind == tsKindVideo && stream.firstPTS >= 0 && stream.lastPTS > stream.firstPTS {
			return float64(stream.lastPTS-stream.firstPTS) / 90000.0
		}
	}
	for _, stream := range s.streams {
		if stream.firstPTS >= 0 && stream.lastPTS > stream.firstPTS {
			return float64(stream.lastPTS-stream.firstPTS) / 90000.0
		}
	}
	return 0
}

func detectTSPacketLayout(f *os.File) (int, int, error) {
	sample := make([]byte, 192*8)
	n, err := io.ReadFull(f, sample)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return 0, 0, err
	}
	sample = sample[:n]
	layouts := []struct {
		packet int
		prefix int
	}{
		{192, 4},
		{188, 0},
	}
	for _, layout := range layouts {
		ok := true
		if len(sample) < layout.packet*3 {
			continue
		}
		for off := layout.prefix; off < len(sample); off += layout.packet {
			if sample[off] != 0x47 {
				ok = false
				break
			}
		}
		if ok {
			return layout.packet, layout.prefix, nil
		}
	}
	return 0, 0, fmt.Errorf("ts: unsupported packet layout")
}

func parseTSPacket(pkt []byte, state *tsProbeState) error {
	if len(pkt) < 188 || pkt[0] != 0x47 {
		return fmt.Errorf("invalid ts packet")
	}
	payloadUnitStart := pkt[1]&0x40 != 0
	pid := uint16(pkt[1]&0x1F)<<8 | uint16(pkt[2])
	adaptationControl := (pkt[3] >> 4) & 0x3
	if adaptationControl == 0 || adaptationControl == 2 {
		return nil
	}
	offset := 4
	if adaptationControl == 3 {
		if len(pkt) <= offset {
			return nil
		}
		adaptLen := int(pkt[offset])
		offset += 1 + adaptLen
	}
	if offset >= len(pkt) {
		return nil
	}
	payload := pkt[offset:]
	switch {
	case pid == 0:
		if payloadUnitStart {
			parsePAT(payload, state)
		}
	case state.pmtPID >= 0 && pid == uint16(state.pmtPID):
		if payloadUnitStart {
			parsePMT(payload, state)
		}
	default:
		stream := state.streams[pid]
		if stream == nil || stream.kind == tsKindUnknown {
			return nil
		}
		parseTSPES(payload, payloadUnitStart, stream)
	}
	return nil
}

func parsePAT(payload []byte, state *tsProbeState) {
	if len(payload) < 8 {
		return
	}
	pointer := int(payload[0])
	if 1+pointer+8 > len(payload) {
		return
	}
	section := payload[1+pointer:]
	if len(section) < 8 || section[0] != 0x00 {
		return
	}
	sectionLen := int(binary.BigEndian.Uint16(section[1:3]) & 0x0FFF)
	if sectionLen < 9 || 3+sectionLen > len(section) {
		return
	}
	data := section[8 : 3+sectionLen-4]
	for len(data) >= 4 {
		programNum := binary.BigEndian.Uint16(data[0:2])
		pid := int(binary.BigEndian.Uint16(data[2:4]) & 0x1FFF)
		if programNum != 0 {
			state.pmtPID = pid
			return
		}
		data = data[4:]
	}
}

func parsePMT(payload []byte, state *tsProbeState) {
	if len(payload) < 12 {
		return
	}
	pointer := int(payload[0])
	if 1+pointer+12 > len(payload) {
		return
	}
	section := payload[1+pointer:]
	if len(section) < 12 || section[0] != 0x02 {
		return
	}
	sectionLen := int(binary.BigEndian.Uint16(section[1:3]) & 0x0FFF)
	if sectionLen < 13 || 3+sectionLen > len(section) {
		return
	}
	progInfoLen := int(binary.BigEndian.Uint16(section[10:12]) & 0x0FFF)
	data := section[12+progInfoLen : 3+sectionLen-4]
	for len(data) >= 5 {
		streamType := data[0]
		pid := binary.BigEndian.Uint16(data[1:3]) & 0x1FFF
		esInfoLen := int(binary.BigEndian.Uint16(data[3:5]) & 0x0FFF)
		if 5+esInfoLen > len(data) {
			return
		}
		kind, codec, channels := classifyTSStream(streamType, data[5:5+esInfoLen])
		if kind != tsKindUnknown {
			existing := state.streams[pid]
			if existing == nil {
				existing = &tsStreamInfo{pid: pid, firstPTS: -1, lastPTS: -1}
				state.streams[pid] = existing
			}
			existing.kind = kind
			if existing.codec == "" {
				existing.codec = codec
			}
			if existing.channels == 0 {
				existing.channels = channels
			}
		}
		data = data[5+esInfoLen:]
	}
}

func classifyTSStream(streamType byte, descriptors []byte) (tsStreamKind, string, int) {
	switch streamType {
	case 0x01, 0x02:
		return tsKindVideo, "MPEG-2", 0
	case 0x03, 0x04:
		return tsKindAudio, "MP3", 2
	case 0x0F, 0x11:
		return tsKindAudio, "AAC", 2
	case 0x1B:
		return tsKindVideo, "AVC", 0
	case 0x24:
		return tsKindVideo, "HEVC", 0
	case 0x80:
		return tsKindAudio, "PCM", 2
	case 0x81:
		return tsKindAudio, "AC-3", 6
	case 0x82:
		return tsKindAudio, "DTS", 6
	case 0x83:
		return tsKindAudio, "TrueHD", 8
	case 0x84:
		return tsKindAudio, "E-AC-3", 6
	case 0x85, 0x86:
		return tsKindAudio, "DTS", 6
	case 0x90:
		return tsKindSubtitle, "PGS", 0
	case 0x06:
		return classifyPrivateTSDescriptors(descriptors)
	default:
		return tsKindUnknown, "", 0
	}
}

func classifyPrivateTSDescriptors(descriptors []byte) (tsStreamKind, string, int) {
	for len(descriptors) >= 2 {
		tag := descriptors[0]
		size := int(descriptors[1])
		if 2+size > len(descriptors) {
			break
		}
		body := descriptors[2 : 2+size]
		switch tag {
		case 0x6A:
			return tsKindAudio, "AC-3", 6
		case 0x7A:
			return tsKindAudio, "E-AC-3", 6
		case 0x7B:
			return tsKindAudio, "DTS", 6
		case 0x56, 0x59:
			return tsKindSubtitle, "PGS", 0
		}
		if len(body) >= 4 {
			reg := string(body[:4])
			switch reg {
			case "AC-3":
				return tsKindAudio, "AC-3", 6
			case "EAC3":
				return tsKindAudio, "E-AC-3", 6
			case "DTS1", "DTS2", "DTS3", "DTSH":
				return tsKindAudio, "DTS", 6
			case "HDMV":
				return tsKindSubtitle, "PGS", 0
			}
		}
		descriptors = descriptors[2+size:]
	}
	return tsKindUnknown, "", 0
}

func parseTSPES(payload []byte, payloadUnitStart bool, stream *tsStreamInfo) {
	if len(payload) == 0 {
		return
	}
	esPayload := payload
	if payloadUnitStart && len(payload) >= 14 && payload[0] == 0x00 && payload[1] == 0x00 && payload[2] == 0x01 {
		ptsDtsFlags := (payload[7] >> 6) & 0x3
		headerDataLen := int(payload[8])
		if ptsDtsFlags&0x2 != 0 && len(payload) >= 14 {
			if pts := parsePESPTS(payload[9:14]); pts >= 0 {
				if stream.firstPTS < 0 {
					stream.firstPTS = pts
				}
				stream.lastPTS = pts
			}
		}
		start := 9 + headerDataLen
		if start < len(payload) {
			esPayload = payload[start:]
		}
	}
	if stream.kind == tsKindVideo && stream.width == 0 {
		switch {
		case strings.EqualFold(stream.codec, "AVC"):
			if width, height, ok := findAVCSPSDimensions(esPayload); ok {
				stream.width = width
				stream.height = height
			}
		case strings.EqualFold(stream.codec, "HEVC"):
			if width, height, ok := findHEVCSPSDimensions(esPayload); ok {
				stream.width = width
				stream.height = height
			}
		case strings.EqualFold(stream.codec, "MPEG-2"):
			if width, height, ok := findMPEG2SequenceDimensions(esPayload); ok {
				stream.width = width
				stream.height = height
			}
		}
	}
}

func parsePESPTS(b []byte) int64 {
	if len(b) < 5 {
		return -1
	}
	return (int64(b[0]>>1&0x07) << 30) |
		(int64(binary.BigEndian.Uint16(b[1:3])>>1) << 15) |
		int64(binary.BigEndian.Uint16(b[3:5])>>1)
}

func findAVCSPSDimensions(data []byte) (int, int, bool) {
	for i := 0; i+5 < len(data); i++ {
		startCodeLen := 0
		if data[i] == 0x00 && data[i+1] == 0x00 && data[i+2] == 0x01 {
			startCodeLen = 3
		} else if i+4 < len(data) && data[i] == 0x00 && data[i+1] == 0x00 && data[i+2] == 0x00 && data[i+3] == 0x01 {
			startCodeLen = 4
		}
		if startCodeLen == 0 {
			continue
		}
		nalStart := i + startCodeLen
		nalType := data[nalStart] & 0x1F
		if nalType != 7 {
			continue
		}
		nalEnd := len(data)
		for j := nalStart + 1; j+3 < len(data); j++ {
			if data[j] == 0x00 && data[j+1] == 0x00 && (data[j+2] == 0x01 || (j+3 < len(data) && data[j+2] == 0x00 && data[j+3] == 0x01)) {
				nalEnd = j
				break
			}
		}
		rbsp := avcRBSP(data[nalStart+1 : nalEnd])
		if width, height, ok := parseAVCSPS(rbsp); ok {
			return width, height, true
		}
	}
	return 0, 0, false
}

func findHEVCSPSDimensions(data []byte) (int, int, bool) {
	for i := 0; i+6 < len(data); i++ {
		startCodeLen := 0
		if data[i] == 0x00 && data[i+1] == 0x00 && data[i+2] == 0x01 {
			startCodeLen = 3
		} else if i+4 < len(data) && data[i] == 0x00 && data[i+1] == 0x00 && data[i+2] == 0x00 && data[i+3] == 0x01 {
			startCodeLen = 4
		}
		if startCodeLen == 0 {
			continue
		}
		nalStart := i + startCodeLen
		nalType := (data[nalStart] >> 1) & 0x3F
		if nalType != 33 {
			continue
		}
		nalEnd := len(data)
		for j := nalStart + 2; j+3 < len(data); j++ {
			if data[j] == 0x00 && data[j+1] == 0x00 && (data[j+2] == 0x01 || (j+3 < len(data) && data[j+2] == 0x00 && data[j+3] == 0x01)) {
				nalEnd = j
				break
			}
		}
		rbsp := avcRBSP(data[nalStart+2 : nalEnd])
		if width, height, ok := parseHEVCSPS(rbsp); ok {
			return width, height, true
		}
	}
	return 0, 0, false
}

func findMPEG2SequenceDimensions(data []byte) (int, int, bool) {
	for i := 0; i+7 < len(data); i++ {
		if data[i] == 0x00 && data[i+1] == 0x00 && data[i+2] == 0x01 && data[i+3] == 0xB3 {
			width := int(data[i+4])<<4 | int(data[i+5]>>4)
			height := int(data[i+5]&0x0F)<<8 | int(data[i+6])
			if width > 0 && height > 0 {
				return width, height, true
			}
		}
	}
	return 0, 0, false
}

func avcRBSP(data []byte) []byte {
	out := make([]byte, 0, len(data))
	zeros := 0
	for _, b := range data {
		if zeros == 2 && b == 0x03 {
			zeros = 0
			continue
		}
		out = append(out, b)
		if b == 0x00 {
			zeros++
		} else {
			zeros = 0
		}
	}
	return out
}

func parseAVCSPS(rbsp []byte) (int, int, bool) {
	br := newBitReader(rbsp)
	profileIDC, ok := br.readBits(8)
	if !ok {
		return 0, 0, false
	}
	if _, ok := br.readBits(8); !ok { // constraints + reserved
		return 0, 0, false
	}
	if _, ok := br.readBits(8); !ok { // level_idc
		return 0, 0, false
	}
	if _, ok := br.readUE(); !ok { // seq_parameter_set_id
		return 0, 0, false
	}
	chromaFormatIDC := uint(1)
	switch profileIDC {
	case 100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135:
		if chromaFormatIDC, ok = br.readUE(); !ok {
			return 0, 0, false
		}
		if chromaFormatIDC == 3 {
			if _, ok := br.readBits(1); !ok {
				return 0, 0, false
			}
		}
		if _, ok := br.readUE(); !ok { // bit_depth_luma_minus8
			return 0, 0, false
		}
		if _, ok := br.readUE(); !ok { // bit_depth_chroma_minus8
			return 0, 0, false
		}
		if _, ok := br.readBits(1); !ok { // qpprime_y_zero_transform_bypass_flag
			return 0, 0, false
		}
		if seqScalingMatrixPresent, ok := br.readBits(1); !ok {
			return 0, 0, false
		} else if seqScalingMatrixPresent == 1 {
			count := 8
			if chromaFormatIDC == 3 {
				count = 12
			}
			for i := 0; i < count; i++ {
				present, ok := br.readBits(1)
				if !ok {
					return 0, 0, false
				}
				if present == 1 {
					size := 16
					if i >= 6 {
						size = 64
					}
					if !skipScalingList(br, size) {
						return 0, 0, false
					}
				}
			}
		}
	}
	if _, ok := br.readUE(); !ok { // log2_max_frame_num_minus4
		return 0, 0, false
	}
	picOrderCntType, ok := br.readUE()
	if !ok {
		return 0, 0, false
	}
	if picOrderCntType == 0 {
		if _, ok := br.readUE(); !ok {
			return 0, 0, false
		}
	} else if picOrderCntType == 1 {
		if _, ok := br.readBits(1); !ok {
			return 0, 0, false
		}
		if _, ok := br.readSE(); !ok {
			return 0, 0, false
		}
		if _, ok := br.readSE(); !ok {
			return 0, 0, false
		}
		numRefFramesInPicOrderCntCycle, ok := br.readUE()
		if !ok {
			return 0, 0, false
		}
		for i := uint(0); i < numRefFramesInPicOrderCntCycle; i++ {
			if _, ok := br.readSE(); !ok {
				return 0, 0, false
			}
		}
	}
	if _, ok := br.readUE(); !ok { // max_num_ref_frames
		return 0, 0, false
	}
	if _, ok := br.readBits(1); !ok { // gaps_in_frame_num_value_allowed_flag
		return 0, 0, false
	}
	picWidthInMbsMinus1, ok := br.readUE()
	if !ok {
		return 0, 0, false
	}
	picHeightInMapUnitsMinus1, ok := br.readUE()
	if !ok {
		return 0, 0, false
	}
	frameMbsOnlyFlag, ok := br.readBits(1)
	if !ok {
		return 0, 0, false
	}
	if frameMbsOnlyFlag == 0 {
		if _, ok := br.readBits(1); !ok {
			return 0, 0, false
		}
	}
	if _, ok := br.readBits(1); !ok { // direct_8x8_inference_flag
		return 0, 0, false
	}
	frameCropLeft, frameCropRight, frameCropTop, frameCropBottom := uint(0), uint(0), uint(0), uint(0)
	frameCroppingFlag, ok := br.readBits(1)
	if !ok {
		return 0, 0, false
	}
	if frameCroppingFlag == 1 {
		if frameCropLeft, ok = br.readUE(); !ok {
			return 0, 0, false
		}
		if frameCropRight, ok = br.readUE(); !ok {
			return 0, 0, false
		}
		if frameCropTop, ok = br.readUE(); !ok {
			return 0, 0, false
		}
		if frameCropBottom, ok = br.readUE(); !ok {
			return 0, 0, false
		}
	}

	width := int((picWidthInMbsMinus1 + 1) * 16)
	height := int((picHeightInMapUnitsMinus1 + 1) * 16)
	if frameMbsOnlyFlag == 0 {
		height *= 2
	}
	cropUnitX, cropUnitY := 1, 2
	switch chromaFormatIDC {
	case 0:
		cropUnitX = 1
		cropUnitY = 2 - int(frameMbsOnlyFlag)
	case 1:
		cropUnitX = 2
		cropUnitY = 2 * (2 - int(frameMbsOnlyFlag))
	case 2:
		cropUnitX = 2
		cropUnitY = 2 - int(frameMbsOnlyFlag)
	case 3:
		cropUnitX = 1
		cropUnitY = 2 - int(frameMbsOnlyFlag)
	}
	width -= int(frameCropLeft+frameCropRight) * cropUnitX
	height -= int(frameCropTop+frameCropBottom) * cropUnitY
	if width <= 0 || height <= 0 {
		return 0, 0, false
	}
	return width, height, true
}

func parseHEVCSPS(rbsp []byte) (int, int, bool) {
	br := newBitReader(rbsp)
	if _, ok := br.readBits(4); !ok { // sps_video_parameter_set_id
		return 0, 0, false
	}
	maxSubLayersMinus1, ok := br.readBits(3)
	if !ok {
		return 0, 0, false
	}
	if _, ok := br.readBits(1); !ok { // sps_temporal_id_nesting_flag
		return 0, 0, false
	}
	if !skipHEVCProfileTierLevel(br, int(maxSubLayersMinus1)) {
		return 0, 0, false
	}
	if _, ok := br.readUE(); !ok { // sps_seq_parameter_set_id
		return 0, 0, false
	}
	chromaFormatIDC, ok := br.readUE()
	if !ok {
		return 0, 0, false
	}
	if chromaFormatIDC == 3 {
		if _, ok := br.readBits(1); !ok { // separate_colour_plane_flag
			return 0, 0, false
		}
	}
	picWidth, ok := br.readUE()
	if !ok {
		return 0, 0, false
	}
	picHeight, ok := br.readUE()
	if !ok {
		return 0, 0, false
	}
	confWinFlag, ok := br.readBits(1)
	if !ok {
		return 0, 0, false
	}
	confLeft, confRight, confTop, confBottom := uint(0), uint(0), uint(0), uint(0)
	if confWinFlag == 1 {
		if confLeft, ok = br.readUE(); !ok {
			return 0, 0, false
		}
		if confRight, ok = br.readUE(); !ok {
			return 0, 0, false
		}
		if confTop, ok = br.readUE(); !ok {
			return 0, 0, false
		}
		if confBottom, ok = br.readUE(); !ok {
			return 0, 0, false
		}
	}
	subWidthC, subHeightC := 1, 1
	switch chromaFormatIDC {
	case 1:
		subWidthC = 2
		subHeightC = 2
	case 2:
		subWidthC = 2
		subHeightC = 1
	case 3:
		subWidthC = 1
		subHeightC = 1
	}
	width := int(picWidth) - int(confLeft+confRight)*subWidthC
	height := int(picHeight) - int(confTop+confBottom)*subHeightC
	if width <= 0 || height <= 0 {
		return 0, 0, false
	}
	return width, height, true
}

func skipHEVCProfileTierLevel(br *bitReader, maxSubLayersMinus1 int) bool {
	if _, ok := br.readBits(2); !ok { // general_profile_space
		return false
	}
	if _, ok := br.readBits(1); !ok { // general_tier_flag
		return false
	}
	if _, ok := br.readBits(5); !ok { // general_profile_idc
		return false
	}
	if _, ok := br.readBits(32); !ok { // general_profile_compatibility_flags
		return false
	}
	if _, ok := br.readBits(16); !ok { // general_constraint_indicator_flags[47:32]
		return false
	}
	if _, ok := br.readBits(16); !ok { // general_constraint_indicator_flags[31:16]
		return false
	}
	if _, ok := br.readBits(16); !ok { // general_constraint_indicator_flags[15:0]
		return false
	}
	if _, ok := br.readBits(8); !ok { // general_level_idc
		return false
	}
	subLayerProfilePresent := make([]uint, maxSubLayersMinus1)
	subLayerLevelPresent := make([]uint, maxSubLayersMinus1)
	for i := 0; i < maxSubLayersMinus1; i++ {
		var ok bool
		if subLayerProfilePresent[i], ok = br.readBits(1); !ok {
			return false
		}
		if subLayerLevelPresent[i], ok = br.readBits(1); !ok {
			return false
		}
	}
	if maxSubLayersMinus1 > 0 {
		for i := maxSubLayersMinus1; i < 8; i++ {
			if _, ok := br.readBits(2); !ok {
				return false
			}
		}
	}
	for i := 0; i < maxSubLayersMinus1; i++ {
		if subLayerProfilePresent[i] == 1 {
			if _, ok := br.readBits(2); !ok {
				return false
			}
			if _, ok := br.readBits(1); !ok {
				return false
			}
			if _, ok := br.readBits(5); !ok {
				return false
			}
			if _, ok := br.readBits(32); !ok {
				return false
			}
			if _, ok := br.readBits(16); !ok {
				return false
			}
			if _, ok := br.readBits(16); !ok {
				return false
			}
			if _, ok := br.readBits(16); !ok {
				return false
			}
		}
		if subLayerLevelPresent[i] == 1 {
			if _, ok := br.readBits(8); !ok {
				return false
			}
		}
	}
	return true
}

type bitReader struct {
	data []byte
	pos  int
}

func newBitReader(data []byte) *bitReader {
	return &bitReader{data: data}
}

func (b *bitReader) readBits(n int) (uint, bool) {
	if n <= 0 || b.pos+n > len(b.data)*8 {
		return 0, false
	}
	var out uint
	for i := 0; i < n; i++ {
		byteIdx := (b.pos + i) / 8
		bitIdx := 7 - ((b.pos + i) % 8)
		out = (out << 1) | uint((b.data[byteIdx]>>bitIdx)&1)
	}
	b.pos += n
	return out, true
}

func (b *bitReader) readUE() (uint, bool) {
	zeros := 0
	for {
		bit, ok := b.readBits(1)
		if !ok {
			return 0, false
		}
		if bit == 1 {
			break
		}
		zeros++
	}
	if zeros == 0 {
		return 0, true
	}
	value, ok := b.readBits(zeros)
	if !ok {
		return 0, false
	}
	return (1 << zeros) - 1 + value, true
}

func (b *bitReader) readSE() (int, bool) {
	v, ok := b.readUE()
	if !ok {
		return 0, false
	}
	n := int(v)
	if n%2 == 0 {
		return -(n / 2), true
	}
	return (n + 1) / 2, true
}

func skipScalingList(br *bitReader, size int) bool {
	lastScale := 8
	nextScale := 8
	for i := 0; i < size; i++ {
		if nextScale != 0 {
			deltaScale, ok := br.readSE()
			if !ok {
				return false
			}
			nextScale = (lastScale + deltaScale + 256) % 256
		}
		if nextScale != 0 {
			lastScale = nextScale
		}
	}
	return true
}

func probeAVIMetadata(fullPath string) (map[string]string, error) {
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < 12 {
		return nil, fmt.Errorf("avi: file too small")
	}

	header := make([]byte, 12)
	if _, err := io.ReadFull(f, header); err != nil {
		return nil, err
	}
	if string(header[:4]) != "RIFF" || string(header[8:12]) != "AVI " {
		return nil, fmt.Errorf("avi: invalid header")
	}

	state := aviProbeState{}
	if err := parseAVIChunks(f, info.Size(), &state); err != nil {
		return nil, err
	}

	fields := map[string]string{}
	if state.videoCodec != "" {
		fields["video_format"] = state.videoCodec
	}
	if state.audioCodec != "" {
		fields["audio_format"] = state.audioCodec
	}
	if state.subtitleCodec != "" {
		fields["subtitle_format"] = state.subtitleCodec
	}
	if state.width > 0 {
		fields["width"] = strconv.Itoa(state.width)
	}
	if state.height > 0 {
		fields["height"] = strconv.Itoa(state.height)
	}
	if state.audioChannels > 0 {
		fields["channels"] = strconv.Itoa(state.audioChannels)
	}
	if state.durationSeconds > 0 {
		fields["duration"] = formatFloatSeconds(state.durationSeconds)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("avi: no usable metadata")
	}
	deriveMediaInfoFields(fields)
	return fields, nil
}

type aviProbeState struct {
	videoCodec      string
	audioCodec      string
	subtitleCodec   string
	width           int
	height          int
	audioChannels   int
	durationSeconds float64
}

type aviStreamHeader struct {
	streamType string
	handler    string
}

func parseAVIChunks(r io.ReadSeeker, end int64, state *aviProbeState) error {
	var current aviStreamHeader
	for {
		pos, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if pos+8 > end {
			return nil
		}
		head := make([]byte, 8)
		if _, err := io.ReadFull(r, head); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		chunkType := string(head[:4])
		chunkSize := int64(binary.LittleEndian.Uint32(head[4:8]))
		next := pos + 8 + chunkSize
		if chunkSize%2 == 1 {
			next++
		}
		switch chunkType {
		case "LIST":
			listTypeBytes := make([]byte, 4)
			if _, err := io.ReadFull(r, listTypeBytes); err != nil {
				return err
			}
			listType := string(listTypeBytes)
			listEnd := pos + 8 + chunkSize
			if listType == "strl" {
				current = aviStreamHeader{}
			}
			if err := parseAVIChunks(r, listEnd, state); err != nil {
				return err
			}
		case "avih":
			buf := make([]byte, chunkSize)
			if _, err := io.ReadFull(r, buf); err != nil {
				return err
			}
			if len(buf) >= 40 {
				microSecPerFrame := binary.LittleEndian.Uint32(buf[0:4])
				totalFrames := binary.LittleEndian.Uint32(buf[16:20])
				width := binary.LittleEndian.Uint32(buf[32:36])
				height := binary.LittleEndian.Uint32(buf[36:40])
				if width > 0 && state.width == 0 {
					state.width = int(width)
				}
				if height > 0 && state.height == 0 {
					state.height = int(height)
				}
				if microSecPerFrame > 0 && totalFrames > 0 && state.durationSeconds == 0 {
					state.durationSeconds = float64(microSecPerFrame) * float64(totalFrames) / 1_000_000.0
				}
			}
		case "strh":
			buf := make([]byte, chunkSize)
			if _, err := io.ReadFull(r, buf); err != nil {
				return err
			}
			if len(buf) >= 8 {
				current.streamType = string(buf[0:4])
				current.handler = strings.TrimRight(string(buf[4:8]), "\x00 ")
				switch current.streamType {
				case "vids":
					if state.videoCodec == "" {
						state.videoCodec = normalizeVideoCodec(current.handler)
					}
				case "txts":
					if state.subtitleCodec == "" {
						state.subtitleCodec = "Text"
					}
				}
			}
		case "strf":
			buf := make([]byte, chunkSize)
			if _, err := io.ReadFull(r, buf); err != nil {
				return err
			}
			switch current.streamType {
			case "auds":
				if len(buf) >= 4 {
					tag := binary.LittleEndian.Uint16(buf[0:2])
					channels := binary.LittleEndian.Uint16(buf[2:4])
					if state.audioCodec == "" {
						state.audioCodec = normalizeAudioCodec(aviAudioCodec(tag))
					}
					if channels > 0 && state.audioChannels == 0 {
						state.audioChannels = int(channels)
					}
				}
			case "vids":
				if len(buf) >= 20 {
					width := int(int32(binary.LittleEndian.Uint32(buf[4:8])))
					height := int(int32(binary.LittleEndian.Uint32(buf[8:12])))
					if width > 0 && state.width == 0 {
						state.width = width
					}
					if height < 0 {
						height = -height
					}
					if height > 0 && state.height == 0 {
						state.height = height
					}
					if state.videoCodec == "" && len(buf) >= 20 {
						codec := strings.TrimRight(string(buf[16:20]), "\x00 ")
						state.videoCodec = normalizeVideoCodec(codec)
					}
				}
			}
		default:
			if _, err := r.Seek(chunkSize, io.SeekCurrent); err != nil {
				return err
			}
		}
		if _, err := r.Seek(next, io.SeekStart); err != nil {
			return err
		}
	}
}

func probeMP4Metadata(fullPath string) (map[string]string, error) {
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	state := mp4ProbeState{}
	if err := parseMP4Boxes(f, 0, info.Size(), &state, nil); err != nil {
		return nil, err
	}

	fields := map[string]string{}
	if track := state.bestVideoTrack(); track != nil {
		if track.codec != "" {
			fields["video_format"] = normalizeVideoCodec(track.codec)
		}
		if track.width > 0 {
			fields["width"] = strconv.Itoa(track.width)
		}
		if track.height > 0 {
			fields["height"] = strconv.Itoa(track.height)
		}
		if track.durationSeconds > 0 {
			fields["duration"] = formatFloatSeconds(track.durationSeconds)
		}
	}
	if track := state.bestAudioTrack(); track != nil {
		if track.codec != "" {
			fields["audio_format"] = normalizeAudioCodec(track.codec)
		}
		if track.channels > 0 {
			fields["channels"] = strconv.Itoa(track.channels)
		}
		if fields["duration"] == "" && track.durationSeconds > 0 {
			fields["duration"] = formatFloatSeconds(track.durationSeconds)
		}
	}
	if track := state.bestSubtitleTrack(); track != nil && track.codec != "" {
		fields["subtitle_format"] = normalizeSubtitleCodec(track.codec)
	}
	if fields["duration"] == "" && state.durationSeconds > 0 {
		fields["duration"] = formatFloatSeconds(state.durationSeconds)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("mp4: no usable metadata")
	}
	deriveMediaInfoFields(fields)
	return fields, nil
}

type mp4ProbeState struct {
	durationSeconds float64
	tracks          []mp4Track
}

type mp4Track struct {
	handler         string
	codec           string
	channels        int
	width           int
	height          int
	durationSeconds float64
}

func (s *mp4ProbeState) bestVideoTrack() *mp4Track {
	for i := range s.tracks {
		if s.tracks[i].handler == "vide" {
			return &s.tracks[i]
		}
	}
	return nil
}

func (s *mp4ProbeState) bestAudioTrack() *mp4Track {
	for i := range s.tracks {
		if s.tracks[i].handler == "soun" {
			return &s.tracks[i]
		}
	}
	return nil
}

func (s *mp4ProbeState) bestSubtitleTrack() *mp4Track {
	for i := range s.tracks {
		switch s.tracks[i].handler {
		case "text", "sbtl", "subt", "clcp":
			return &s.tracks[i]
		}
	}
	return nil
}

func parseMP4Boxes(r io.ReadSeeker, start, end int64, state *mp4ProbeState, track *mp4Track) error {
	pos := start
	for pos+8 <= end {
		if _, err := r.Seek(pos, io.SeekStart); err != nil {
			return err
		}
		header := make([]byte, 8)
		if _, err := io.ReadFull(r, header); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		size := int64(binary.BigEndian.Uint32(header[0:4]))
		boxType := string(header[4:8])
		headerSize := int64(8)
		if size == 1 {
			ext := make([]byte, 8)
			if _, err := io.ReadFull(r, ext); err != nil {
				return err
			}
			size = int64(binary.BigEndian.Uint64(ext))
			headerSize = 16
		} else if size == 0 {
			size = end - pos
		}
		if size < headerSize || pos+size > end {
			return nil
		}
		payloadStart := pos + headerSize
		payloadEnd := pos + size
		switch boxType {
		case "moov", "mdia", "minf", "stbl":
			if err := parseMP4Boxes(r, payloadStart, payloadEnd, state, track); err != nil {
				return err
			}
		case "trak":
			t := mp4Track{}
			if err := parseMP4Boxes(r, payloadStart, payloadEnd, state, &t); err != nil {
				return err
			}
			state.tracks = append(state.tracks, t)
		case "mvhd":
			duration, err := parseMP4MovieHeader(r, payloadStart, payloadEnd)
			if err == nil && duration > 0 && state.durationSeconds == 0 {
				state.durationSeconds = duration
			}
		case "tkhd":
			if track != nil {
				width, height, err := parseMP4TrackHeader(r, payloadStart, payloadEnd)
				if err == nil {
					if width > 0 {
						track.width = width
					}
					if height > 0 {
						track.height = height
					}
				}
			}
		case "mdhd":
			if track != nil {
				duration, err := parseMP4MediaHeader(r, payloadStart, payloadEnd)
				if err == nil && duration > 0 {
					track.durationSeconds = duration
				}
			}
		case "hdlr":
			if track != nil {
				handler, err := parseMP4Handler(r, payloadStart, payloadEnd)
				if err == nil && handler != "" {
					track.handler = handler
				}
			}
		case "stsd":
			if track != nil {
				codec, channels, width, height, err := parseMP4SampleDescription(r, payloadStart, payloadEnd, track.handler)
				if err == nil {
					if codec != "" {
						track.codec = codec
					}
					if channels > 0 {
						track.channels = channels
					}
					if width > 0 && track.width == 0 {
						track.width = width
					}
					if height > 0 && track.height == 0 {
						track.height = height
					}
				}
			}
		}
		pos += size
	}
	return nil
}

func parseMP4MovieHeader(r io.ReadSeeker, start, end int64) (float64, error) {
	buf, err := readBoxPayload(r, start, end, 32)
	if err != nil {
		return 0, err
	}
	version := buf[0]
	if version == 1 {
		if len(buf) < 32 {
			return 0, fmt.Errorf("mvhd: short version 1")
		}
		timescale := binary.BigEndian.Uint32(buf[20:24])
		duration := binary.BigEndian.Uint64(buf[24:32])
		return scaleDuration(timescale, duration), nil
	}
	if len(buf) < 20 {
		return 0, fmt.Errorf("mvhd: short version 0")
	}
	timescale := binary.BigEndian.Uint32(buf[12:16])
	duration := uint64(binary.BigEndian.Uint32(buf[16:20]))
	return scaleDuration(timescale, duration), nil
}

func parseMP4TrackHeader(r io.ReadSeeker, start, end int64) (int, int, error) {
	buf, err := readBoxPayload(r, start, end, 92)
	if err != nil {
		return 0, 0, err
	}
	version := buf[0]
	var widthOff, heightOff int
	if version == 1 {
		if len(buf) < 104 {
			return 0, 0, fmt.Errorf("tkhd: short version 1")
		}
		widthOff = 88
		heightOff = 92
	} else {
		if len(buf) < 84 {
			return 0, 0, fmt.Errorf("tkhd: short version 0")
		}
		widthOff = 76
		heightOff = 80
	}
	width := int(binary.BigEndian.Uint32(buf[widthOff:widthOff+4]) >> 16)
	height := int(binary.BigEndian.Uint32(buf[heightOff:heightOff+4]) >> 16)
	return width, height, nil
}

func parseMP4MediaHeader(r io.ReadSeeker, start, end int64) (float64, error) {
	buf, err := readBoxPayload(r, start, end, 32)
	if err != nil {
		return 0, err
	}
	version := buf[0]
	if version == 1 {
		if len(buf) < 32 {
			return 0, fmt.Errorf("mdhd: short version 1")
		}
		timescale := binary.BigEndian.Uint32(buf[20:24])
		duration := binary.BigEndian.Uint64(buf[24:32])
		return scaleDuration(timescale, duration), nil
	}
	if len(buf) < 20 {
		return 0, fmt.Errorf("mdhd: short version 0")
	}
	timescale := binary.BigEndian.Uint32(buf[12:16])
	duration := uint64(binary.BigEndian.Uint32(buf[16:20]))
	return scaleDuration(timescale, duration), nil
}

func parseMP4Handler(r io.ReadSeeker, start, end int64) (string, error) {
	buf, err := readBoxPayload(r, start, end, 12)
	if err != nil {
		return "", err
	}
	if len(buf) < 12 {
		return "", fmt.Errorf("hdlr: short")
	}
	return string(buf[8:12]), nil
}

func parseMP4SampleDescription(r io.ReadSeeker, start, end int64, handler string) (string, int, int, int, error) {
	buf, err := readBoxPayload(r, start, end, int(end-start))
	if err != nil {
		return "", 0, 0, 0, err
	}
	if len(buf) < 16 {
		return "", 0, 0, 0, fmt.Errorf("stsd: short")
	}
	entryCount := binary.BigEndian.Uint32(buf[4:8])
	if entryCount == 0 {
		return "", 0, 0, 0, fmt.Errorf("stsd: no entries")
	}
	entrySize := int(binary.BigEndian.Uint32(buf[8:12]))
	if entrySize < 8 || 8+entrySize > len(buf) {
		return "", 0, 0, 0, fmt.Errorf("stsd: invalid entry")
	}
	entryType := string(buf[12:16])
	channels := 0
	width := 0
	height := 0
	switch handler {
	case "soun":
		if 8+28 <= len(buf) {
			channels = int(binary.BigEndian.Uint16(buf[32:34]))
		}
	case "text", "sbtl", "subt", "clcp":
		return normalizeSubtitleCodec(entryType), 0, 0, 0, nil
	case "vide":
		if 8+28 <= len(buf) {
			width = int(binary.BigEndian.Uint16(buf[32:34]))
			height = int(binary.BigEndian.Uint16(buf[34:36]))
		}
	}
	return entryType, channels, width, height, nil
}

func readBoxPayload(r io.ReadSeeker, start, end int64, minLen int) ([]byte, error) {
	if _, err := r.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}
	size := end - start
	if size < 0 {
		return nil, fmt.Errorf("negative payload")
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	if len(buf) < minLen {
		return nil, fmt.Errorf("short payload")
	}
	return buf, nil
}

func probeMKVMetadata(fullPath string) (map[string]string, error) {
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	state := mkvProbeState{timecodeScale: 1_000_000}
	if err := parseMKVElements(f, info.Size(), &state); err != nil {
		return nil, err
	}

	fields := map[string]string{}
	if state.videoCodec != "" {
		fields["video_format"] = normalizeVideoCodec(state.videoCodec)
	}
	if state.audioCodec != "" {
		fields["audio_format"] = normalizeAudioCodec(state.audioCodec)
	}
	if state.subtitleCodec != "" {
		fields["subtitle_format"] = normalizeSubtitleCodec(state.subtitleCodec)
	}
	if state.width > 0 {
		fields["width"] = strconv.Itoa(state.width)
	}
	if state.height > 0 {
		fields["height"] = strconv.Itoa(state.height)
	}
	if state.audioChannels > 0 {
		fields["channels"] = strconv.Itoa(state.audioChannels)
	}
	if state.durationSeconds > 0 {
		fields["duration"] = formatFloatSeconds(state.durationSeconds)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("mkv: no usable metadata")
	}
	deriveMediaInfoFields(fields)
	return fields, nil
}

type mkvProbeState struct {
	timecodeScale   uint64
	durationUnits   float64
	durationSeconds float64
	videoCodec      string
	audioCodec      string
	subtitleCodec   string
	width           int
	height          int
	audioChannels   int
}

func parseMKVElements(r io.ReadSeeker, fileSize int64, state *mkvProbeState) error {
	for {
		pos, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if pos >= fileSize {
			break
		}
		id, idLen, err := readEBMLID(r)
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		size, sizeLen, unknown, err := readEBMLSize(r)
		if err != nil {
			return err
		}
		payloadStart := pos + int64(idLen+sizeLen)
		payloadEnd := payloadStart + int64(size)
		if unknown || payloadEnd > fileSize {
			payloadEnd = fileSize
		}
		switch id {
		case 0x1549A966:
			if err := parseMKVInfo(r, payloadEnd, state); err != nil {
				return err
			}
		case 0x1654AE6B:
			if err := parseMKVTracks(r, payloadEnd, state); err != nil {
				return err
			}
		case 0x1F43B675:
			if state.durationSeconds > 0 && state.videoCodec != "" {
				return nil
			}
		}
		if _, err := r.Seek(payloadEnd, io.SeekStart); err != nil {
			return err
		}
	}
	if state.durationUnits > 0 && state.durationSeconds == 0 {
		state.durationSeconds = state.durationUnits * float64(state.timecodeScale) / 1_000_000_000.0
	}
	return nil
}

func parseMKVInfo(r io.ReadSeeker, end int64, state *mkvProbeState) error {
	for {
		pos, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if pos >= end {
			return nil
		}
		id, _, err := readEBMLID(r)
		if err != nil {
			return err
		}
		size, _, _, err := readEBMLSize(r)
		if err != nil {
			return err
		}
		switch id {
		case 0x2AD7B1:
			if v, err := readEBMLUInt(r, size); err == nil && v > 0 {
				state.timecodeScale = v
			}
		case 0x4489:
			if v, err := readEBMLFloat(r, size); err == nil && v > 0 {
				state.durationUnits = v
			}
		default:
			if _, err := r.Seek(int64(size), io.SeekCurrent); err != nil {
				return err
			}
		}
	}
}

func parseMKVTracks(r io.ReadSeeker, end int64, state *mkvProbeState) error {
	for {
		pos, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if pos >= end {
			return nil
		}
		id, _, err := readEBMLID(r)
		if err != nil {
			return err
		}
		size, _, _, err := readEBMLSize(r)
		if err != nil {
			return err
		}
		payloadStart, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		payloadEnd := payloadStart + int64(size)
		switch id {
		case 0xAE:
			if err := parseMKVTrackEntry(r, payloadEnd, state); err != nil {
				return err
			}
		default:
			if _, err := r.Seek(int64(size), io.SeekCurrent); err != nil {
				return err
			}
		}
	}
}

func parseMKVTrackEntry(r io.ReadSeeker, end int64, state *mkvProbeState) error {
	trackType := uint64(0)
	codecID := ""
	width := 0
	height := 0
	channels := 0

	for {
		pos, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if pos >= end {
			break
		}
		id, _, err := readEBMLID(r)
		if err != nil {
			return err
		}
		size, _, _, err := readEBMLSize(r)
		if err != nil {
			return err
		}
		switch id {
		case 0x83:
			trackType, err = readEBMLUInt(r, size)
			if err != nil {
				return err
			}
		case 0x86:
			codecID, err = readEBMLString(r, size)
			if err != nil {
				return err
			}
		case 0xE0:
			w, h, err := parseMKVVideo(r, int64(size))
			if err != nil {
				return err
			}
			if w > 0 {
				width = w
			}
			if h > 0 {
				height = h
			}
		case 0xE1:
			c, err := parseMKVAudio(r, int64(size))
			if err != nil {
				return err
			}
			if c > 0 {
				channels = c
			}
		default:
			if _, err := r.Seek(int64(size), io.SeekCurrent); err != nil {
				return err
			}
		}
	}

	switch trackType {
	case 1:
		if state.videoCodec == "" {
			state.videoCodec = codecID
		}
		if state.width == 0 {
			state.width = width
		}
		if state.height == 0 {
			state.height = height
		}
	case 2:
		if state.audioCodec == "" {
			state.audioCodec = codecID
		}
		if state.audioChannels == 0 {
			state.audioChannels = channels
		}
	case 17:
		if state.subtitleCodec == "" {
			state.subtitleCodec = codecID
		}
	}
	return nil
}

func parseMKVVideo(r io.ReadSeeker, size int64) (int, int, error) {
	end, err := r.Seek(size, io.SeekCurrent)
	if err != nil {
		return 0, 0, err
	}
	if _, err := r.Seek(-size, io.SeekCurrent); err != nil {
		return 0, 0, err
	}
	width := 0
	height := 0
	for {
		pos, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, 0, err
		}
		if pos >= end {
			return width, height, nil
		}
		id, _, err := readEBMLID(r)
		if err != nil {
			return 0, 0, err
		}
		sz, _, _, err := readEBMLSize(r)
		if err != nil {
			return 0, 0, err
		}
		switch id {
		case 0xB0:
			v, err := readEBMLUInt(r, sz)
			if err != nil {
				return 0, 0, err
			}
			width = int(v)
		case 0xBA:
			v, err := readEBMLUInt(r, sz)
			if err != nil {
				return 0, 0, err
			}
			height = int(v)
		default:
			if _, err := r.Seek(int64(sz), io.SeekCurrent); err != nil {
				return 0, 0, err
			}
		}
	}
}

func parseMKVAudio(r io.ReadSeeker, size int64) (int, error) {
	end, err := r.Seek(size, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if _, err := r.Seek(-size, io.SeekCurrent); err != nil {
		return 0, err
	}
	channels := 0
	for {
		pos, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, err
		}
		if pos >= end {
			return channels, nil
		}
		id, _, err := readEBMLID(r)
		if err != nil {
			return 0, err
		}
		sz, _, _, err := readEBMLSize(r)
		if err != nil {
			return 0, err
		}
		switch id {
		case 0x9F:
			v, err := readEBMLUInt(r, sz)
			if err != nil {
				return 0, err
			}
			channels = int(v)
		default:
			if _, err := r.Seek(int64(sz), io.SeekCurrent); err != nil {
				return 0, err
			}
		}
	}
}

func readEBMLID(r io.Reader) (uint64, int, error) {
	var first [1]byte
	if _, err := io.ReadFull(r, first[:]); err != nil {
		return 0, 0, err
	}
	mask := byte(0x80)
	length := 1
	for length <= 4 && first[0]&mask == 0 {
		mask >>= 1
		length++
	}
	if length > 4 {
		return 0, 0, fmt.Errorf("invalid ebml id")
	}
	value := uint64(first[0])
	buf := make([]byte, length-1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, 0, err
	}
	for _, b := range buf {
		value = (value << 8) | uint64(b)
	}
	return value, length, nil
}

func readEBMLSize(r io.Reader) (uint64, int, bool, error) {
	var first [1]byte
	if _, err := io.ReadFull(r, first[:]); err != nil {
		return 0, 0, false, err
	}
	mask := byte(0x80)
	length := 1
	for length <= 8 && first[0]&mask == 0 {
		mask >>= 1
		length++
	}
	if length > 8 {
		return 0, 0, false, fmt.Errorf("invalid ebml size")
	}
	value := uint64(first[0] &^ mask)
	buf := make([]byte, length-1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, 0, false, err
	}
	unknown := value == uint64(mask-1)
	for _, b := range buf {
		value = (value << 8) | uint64(b)
		if unknown && b != 0xFF {
			unknown = false
		}
	}
	return value, length, unknown, nil
}

func readEBMLUInt(r io.Reader, size uint64) (uint64, error) {
	if size == 0 || size > 8 {
		return 0, fmt.Errorf("invalid ebml uint size")
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	var v uint64
	for _, b := range buf {
		v = (v << 8) | uint64(b)
	}
	return v, nil
}

func readEBMLFloat(r io.Reader, size uint64) (float64, error) {
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	switch size {
	case 4:
		return float64(math.Float32frombits(binary.BigEndian.Uint32(buf))), nil
	case 8:
		return math.Float64frombits(binary.BigEndian.Uint64(buf)), nil
	default:
		return 0, fmt.Errorf("unsupported ebml float size")
	}
}

func readEBMLString(r io.Reader, size uint64) (string, error) {
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return strings.TrimRight(string(buf), "\x00 "), nil
}

func scaleDuration(timescale uint32, duration uint64) float64 {
	if timescale == 0 || duration == 0 {
		return 0
	}
	return float64(duration) / float64(timescale)
}

func formatFloatSeconds(seconds float64) string {
	if seconds <= 0 {
		return ""
	}
	return strconv.FormatFloat(seconds, 'f', 3, 64)
}

func normalizeVideoCodec(codec string) string {
	codec = strings.TrimSpace(strings.Trim(codec, "\x00"))
	switch strings.ToLower(codec) {
	case "h264", "avc1", "avc3", "v_mpeg4/iso/avc":
		return "AVC"
	case "h265", "hev1", "hvc1", "v_mpegh/iso/hevc":
		return "HEVC"
	case "av01", "v_av1":
		return "AV1"
	case "vp09", "v_vp9":
		return "VP9"
	case "xvid", "divx", "dx50", "mp4v", "v_mpeg4/iso/asp", "v_mpeg4/iso/sp":
		return "MPEG-4 Visual"
	case "x264":
		return "AVC"
	case "x265":
		return "HEVC"
	default:
		return strings.ToUpper(codec)
	}
}

func normalizeAudioCodec(codec string) string {
	codec = strings.TrimSpace(strings.Trim(codec, "\x00"))
	switch strings.ToLower(codec) {
	case "mp3", "0x0055":
		return "MP3"
	case "aac", "mp4a", "a_aac":
		return "AAC"
	case "ac-3", "ac3", "a_ac3", "0x2000":
		return "AC-3"
	case "e-ac-3", "ec-3", "ec3", "a_eac3":
		return "E-AC-3"
	case "dts", "a_dts":
		return "DTS"
	case "flac", "a_flac":
		return "FLAC"
	case "opus", "a_opus":
		return "Opus"
	case "vorbis", "a_vorbis":
		return "Vorbis"
	case "pcm", "1":
		return "PCM"
	default:
		return strings.ToUpper(codec)
	}
}

func normalizeSubtitleCodec(codec string) string {
	codec = strings.TrimSpace(strings.Trim(codec, "\x00"))
	switch strings.ToLower(codec) {
	case "", "utf-8":
		return codec
	case "s_text/utf8", "utf8":
		return "UTF-8"
	case "s_text/ass", "ass":
		return "ASS"
	case "s_text/ssa", "ssa":
		return "SSA"
	case "s_hdmv/pgs", "pgs":
		return "PGS"
	case "tx3g":
		return "TX3G"
	case "wvtt":
		return "WebVTT"
	case "stpp":
		return "TTML"
	default:
		return strings.ToUpper(codec)
	}
}

func aviAudioCodec(tag uint16) string {
	switch tag {
	case 0x0001:
		return "PCM"
	case 0x0055:
		return "MP3"
	case 0x00FF:
		return "AAC"
	case 0x2000:
		return "AC-3"
	case 0x2001:
		return "DTS"
	default:
		return fmt.Sprintf("0x%04x", tag)
	}
}
