package core

import "strings"

func (s *Session) abortPendingPassthroughSetup(reason string) {
	if s == nil || s.PassthruXferIdx == 0 {
		return
	}
	aborter, ok := s.MasterManager.(transferAborter)
	if !ok {
		return
	}
	slaveName, ok := s.PassthruSlave.(string)
	if !ok || strings.TrimSpace(slaveName) == "" {
		return
	}
	if strings.TrimSpace(reason) == "" {
		reason = "prepared passthrough cleared"
	}
	aborter.AbortTransfer(slaveName, s.PassthruXferIdx, reason)
}

func (s *Session) clearPreparedTransferState() {
	if s == nil {
		return
	}
	s.abortPendingPassthroughSetup("prepared transfer cleared")
	if s.DataListen != nil {
		_ = s.DataListen.Close()
		s.DataListen = nil
	}
	s.ActiveAddr = ""
	s.PassthruSlave = nil
	s.PassthruXferIdx = 0
	s.PretCmd = ""
	s.PretArg = ""
	s.RestOffset = 0
	s.nextDataTLSClientMode = false
}

func (s *Session) clearPassiveTransferSetup() {
	if s == nil {
		return
	}
	s.abortPendingPassthroughSetup("passive transfer setup cleared")
	if s.DataListen != nil {
		_ = s.DataListen.Close()
		s.DataListen = nil
	}
	s.PassthruSlave = nil
	s.PassthruXferIdx = 0
	s.nextDataTLSClientMode = false
}

func (s *Session) clearActiveTransferSetup() {
	if s == nil {
		return
	}
	s.ActiveAddr = ""
}
