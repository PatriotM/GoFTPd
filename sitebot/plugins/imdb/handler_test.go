package imdb

import "testing"

func TestNormalizePlotText(t *testing.T) {
	got := normalizePlotText("What was supposed to be fun &amp; games\nbecomes a long story", 24)
	want := "What was supposed..."
	if got != want {
		t.Fatalf("normalizePlotText() = %q, want %q", got, want)
	}
}
