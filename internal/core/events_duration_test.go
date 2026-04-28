package core

import "testing"

func TestFormatRaceDurationKeepsSubMinuteFractionalSeconds(t *testing.T) {
	if got, want := formatRaceDuration(18040), "18.0400s"; got != want {
		t.Fatalf("formatRaceDuration(18040) = %q, want %q", got, want)
	}
}

func TestFormatRaceDurationUsesMinuteFormatAboveSixtySeconds(t *testing.T) {
	if got, want := formatRaceDuration(393331), "6m33s"; got != want {
		t.Fatalf("formatRaceDuration(393331) = %q, want %q", got, want)
	}
}
