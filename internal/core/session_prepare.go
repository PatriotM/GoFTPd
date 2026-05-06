package core

func (s *Session) clearPreparedTransferState() {
	if s == nil {
		return
	}
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
