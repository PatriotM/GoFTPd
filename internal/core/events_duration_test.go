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

func TestChooseRaceDurationMsClampsStaleHistoricalWallClock(t *testing.T) {
	users := []VFSRaceUser{
		{Name: "u1", DurationMs: 34000},
		{Name: "u2", DurationMs: 12000},
	}
	if got := chooseRaceDurationMs(149000, users, 1000); got != 34000 {
		t.Fatalf("chooseRaceDurationMs stale wall-clock = %d, want 34000", got)
	}
}

func TestChooseRaceDurationMsKeepsReasonableWallClock(t *testing.T) {
	users := []VFSRaceUser{
		{Name: "u1", DurationMs: 34000},
		{Name: "u2", DurationMs: 12000},
	}
	if got := chooseRaceDurationMs(36000, users, 1000); got != 36000 {
		t.Fatalf("chooseRaceDurationMs reasonable wall-clock = %d, want 36000", got)
	}
}
