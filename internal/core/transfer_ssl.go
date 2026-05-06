package core

func canUseSecureFXP(s *Session) bool {
	if s == nil {
		return false
	}
	return s.IsTLS && s.DataTLS
}
