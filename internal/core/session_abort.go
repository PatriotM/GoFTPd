package core

import "strings"

type transferAborter interface {
	AbortTransfer(slaveName string, transferIndex int32, reason string) bool
}

func (s *Session) abortCurrentTransfer(reason string) bool {
	if s == nil {
		return false
	}
	if strings.TrimSpace(reason) == "" {
		reason = "Transfer aborted"
	}

	aborted := false
	s.stateMu.RLock()
	slaveName := s.TransferSlaveName
	slaveIdx := s.TransferSlaveIdx
	hasTransfer := strings.TrimSpace(s.TransferDirection) != "" && strings.TrimSpace(s.TransferPath) != ""
	preparedSlaveName := ""
	preparedSlaveIdx := s.PassthruXferIdx
	if preparedSlaveIdx != 0 {
		if name, ok := s.PassthruSlave.(string); ok {
			preparedSlaveName = strings.TrimSpace(name)
		}
	}
	s.stateMu.RUnlock()

	if aborter, ok := s.MasterManager.(transferAborter); ok {
		if preparedSlaveName != "" && preparedSlaveIdx != 0 {
			if aborter.AbortTransfer(preparedSlaveName, preparedSlaveIdx, reason) {
				aborted = true
			}
		}
		if hasTransfer && strings.TrimSpace(slaveName) != "" && slaveIdx != 0 && (slaveName != preparedSlaveName || slaveIdx != preparedSlaveIdx) {
			if aborter.AbortTransfer(slaveName, slaveIdx, reason) {
				aborted = true
			}
		}
	}

	if s.DataListen != nil {
		_ = s.DataListen.Close()
		s.DataListen = nil
		aborted = true
	}
	if s.ActiveAddr != "" || s.PassthruSlave != nil || s.PassthruXferIdx != 0 || s.PretCmd != "" || s.PretArg != "" {
		aborted = true
	}
	s.ActiveAddr = ""
	s.PassthruSlave = nil
	s.PassthruXferIdx = 0
	s.PretCmd = ""
	s.PretArg = ""
	s.endTransfer()
	return aborted || hasTransfer
}
