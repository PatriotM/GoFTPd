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

func TestZipPayloadSizesLookComplete(t *testing.T) {
	tests := []struct {
		name    string
		entries []MasterFileEntry
		want    bool
	}{
		{
			name: "uniform with one smaller tail",
			entries: []MasterFileEntry{
				{Name: "a.zip", Size: 5000},
				{Name: "b.zip", Size: 5000},
				{Name: "c.zip", Size: 3200},
			},
			want: true,
		},
		{
			name: "multiple undersized parts",
			entries: []MasterFileEntry{
				{Name: "a.zip", Size: 5000},
				{Name: "b.zip", Size: 3200},
				{Name: "c.zip", Size: 2800},
			},
			want: false,
		},
		{
			name: "all uniform",
			entries: []MasterFileEntry{
				{Name: "a.zip", Size: 5000},
				{Name: "b.zip", Size: 5000},
				{Name: "c.zip", Size: 5000},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := zipPayloadSizesLookComplete(tt.entries); got != tt.want {
				t.Fatalf("zipPayloadSizesLookComplete() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestZipDirCompleteRequiresSaneSizes(t *testing.T) {
	entries := []MasterFileEntry{
		{Name: "a.zip", Size: 5000},
		{Name: "b.zip", Size: 3200},
		{Name: "c.zip", Size: 2800},
	}
	if zipDirComplete(nil, "/0DAY/test", entries, 3) {
		t.Fatalf("expected zipDirComplete to reject multiple undersized zip parts")
	}
}
