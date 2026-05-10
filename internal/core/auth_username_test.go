package core

import "testing"

func TestNormalizeLoginUsername(t *testing.T) {
	tests := map[string]string{
		"Chimichanga":    "Chimichanga",
		"/Chimichanga":   "Chimichanga",
		"\\Chimichanga":  "Chimichanga",
		" users/Finity ": "Finity",
		"../probe":       "probe",
		".":              "",
		"/":              "",
		"   ":            "",
	}

	for in, want := range tests {
		if got := normalizeLoginUsername(in); got != want {
			t.Fatalf("normalizeLoginUsername(%q) = %q, want %q", in, got, want)
		}
	}
}
