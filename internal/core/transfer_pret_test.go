package core

import "testing"

func TestHasPretForPassiveStandaloneDoesNotRequirePret(t *testing.T) {
	s := &Session{Config: &Config{Mode: "standalone"}}
	if !hasPretForPassive(s) {
		t.Fatalf("standalone session should not require PRET before PASV/CPSV")
	}
}

func TestHasPretForPassiveMasterRequiresPret(t *testing.T) {
	s := &Session{
		Config:        &Config{Mode: "master"},
		MasterManager: struct{}{},
	}
	if hasPretForPassive(s) {
		t.Fatalf("master session should require PRET before PASV/CPSV when no PRET command was issued")
	}
	s.PretCmd = "RETR"
	if !hasPretForPassive(s) {
		t.Fatalf("master session with PRET should allow PASV/CPSV")
	}
}
