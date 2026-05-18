package core

import "testing"

func TestZipExpectedPartsFromDIZContentMatchesDrFTPDStyleMarkers(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    int
	}{
		{name: "square brackets", content: "[01/15]", want: 15},
		{name: "angle brackets with o/x digits", content: "<ox/o9>", want: 9},
		{name: "spaces around separator", content: "( 1 / 12 )", want: 12},
		{name: "missing marker", content: "no file count here", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := zipExpectedPartsFromDIZContent([]byte(tt.content)); got != tt.want {
				t.Fatalf("zipExpectedPartsFromDIZContent(%q) = %d, want %d", tt.content, got, tt.want)
			}
		})
	}
}
