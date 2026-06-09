package tvmaze

import "testing"

func TestParseTVNameStopsAtYearTag(t *testing.T) {
	title, season, episode := parseTVName("The.Price.Is.Right.2026.06.01.1080p.WEB.h264-DiRT")
	if title != "The Price Is Right" || season != 0 || episode != 0 {
		t.Fatalf("parseTVName() = %q, %d, %d; want The Price Is Right, 0, 0", title, season, episode)
	}
}

func TestSelectBestTVMazeShowRejectsWeakTitleMatch(t *testing.T) {
	results := []tvmSearchResult{{
		Score: 1,
		Show:  tvmShow{Name: "The Walking Dead"},
	}}
	if got := selectBestTVMazeShow(results, "The Price Is Right"); got != nil {
		t.Fatalf("selectBestTVMazeShow() = %#v, want nil for weak match", got)
	}
}

func TestSelectBestTVMazeShowAcceptsExactTitle(t *testing.T) {
	results := []tvmSearchResult{{
		Score: 1,
		Show:  tvmShow{Name: "The Price Is Right"},
	}}
	got := selectBestTVMazeShow(results, "The Price Is Right")
	if got == nil || got.Name != "The Price Is Right" {
		t.Fatalf("selectBestTVMazeShow() = %#v, want exact title match", got)
	}
}

func TestSelectBestTVMazeShowAcceptsAcronymAlias(t *testing.T) {
	results := []tvmSearchResult{{
		Score: 1,
		Show:  tvmShow{Name: "Law & Order: Special Victims Unit"},
	}}
	got := selectBestTVMazeShow(results, "Law Order SVU")
	if got == nil || got.Name != "Law & Order: Special Victims Unit" {
		t.Fatalf("selectBestTVMazeShow() = %#v, want acronym alias match", got)
	}
}
