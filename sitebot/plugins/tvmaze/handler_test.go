package tvmaze

import "testing"

func TestExtractShowNameStopsAtYearTag(t *testing.T) {
	got := extractShowName("The.Price.Is.Right.2026.06.01.1080p.WEB.h264-DiRT")
	if got != "The Price Is Right" {
		t.Fatalf("extractShowName() = %q, want The Price Is Right", got)
	}
}

func TestSelectBestTVMazeShowRejectsWeakTitleMatch(t *testing.T) {
	results := []tvmazeSearchResult{{
		Score: 1,
		Show:  tvmazeShow{Name: "The Walking Dead"},
	}}
	if got := selectBestTVMazeShow(results, "The Price Is Right"); got != nil {
		t.Fatalf("selectBestTVMazeShow() = %#v, want nil for weak match", got)
	}
}

func TestSelectBestTVMazeShowAcceptsExactTitle(t *testing.T) {
	results := []tvmazeSearchResult{{
		Score: 1,
		Show:  tvmazeShow{Name: "The Price Is Right"},
	}}
	got := selectBestTVMazeShow(results, "The Price Is Right")
	if got == nil || got.Name != "The Price Is Right" {
		t.Fatalf("selectBestTVMazeShow() = %#v, want exact title match", got)
	}
}

func TestSelectBestTVMazeShowAcceptsAcronymAlias(t *testing.T) {
	results := []tvmazeSearchResult{{
		Score: 1,
		Show:  tvmazeShow{Name: "Law & Order: Special Victims Unit"},
	}}
	got := selectBestTVMazeShow(results, "Law Order SVU")
	if got == nil || got.Name != "Law & Order: Special Victims Unit" {
		t.Fatalf("selectBestTVMazeShow() = %#v, want acronym alias match", got)
	}
}
