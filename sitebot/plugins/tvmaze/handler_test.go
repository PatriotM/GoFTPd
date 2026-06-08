package tvmaze

import "testing"

func TestExtractShowNameStopsAtYearTag(t *testing.T) {
	got := extractShowName("The.Price.Is.Right.2026.06.01.1080p.WEB.h264-DiRT")
	if got != "The Price Is Right" {
		t.Fatalf("extractShowName() = %q, want The Price Is Right", got)
	}
}

func TestExtractShowNameStopsBeforeShowYearSeasonTag(t *testing.T) {
	got := extractShowName("Fallout.2024.S02E01.MULTi.2160p.UHD.BluRay.x265-SODAPOP")
	if got != "Fallout" {
		t.Fatalf("extractShowName() = %q, want Fallout", got)
	}
}

func TestExtractTVMazeReleaseQueryKeepsShowYearFilter(t *testing.T) {
	query := extractTVMazeReleaseQuery("Fallout.2024.S02E01.MULTi.2160p.UHD.BluRay.x265-SODAPOP")
	if query.Title != "Fallout" || query.Year != "2024" {
		t.Fatalf("extractTVMazeReleaseQuery() = %#v; want title Fallout, year 2024", query)
	}
}

func TestSelectBestTVMazeShowFollowsSearchOrder(t *testing.T) {
	results := []tvmazeSearchResult{{
		Score: 1,
		Show:  tvmazeShow{Name: "The Walking Dead"},
	}}
	if got := selectBestTVMazeShow(results, "The Price Is Right"); got == nil || got.Name != "The Walking Dead" {
		t.Fatalf("selectBestTVMazeShow() = %#v, want first search result", got)
	}
}

func TestSelectBestTVMazeShowAppliesYearFilter(t *testing.T) {
	results := []tvmazeSearchResult{{
		Score: 1,
		Show:  tvmazeShow{Name: "Fallout", Premiered: "1997-09-14"},
	}, {
		Score: 1,
		Show:  tvmazeShow{Name: "Fallout", Premiered: "2024-04-10"},
	}}
	got := selectBestTVMazeShow(results, "Fallout", tvmazeMatchCriteria{Year: "2024"})
	if got == nil || got.Premiered != "2024-04-10" {
		t.Fatalf("selectBestTVMazeShow() = %#v, want 2024 match", got)
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

func TestSelectBestTVMazeShowAcceptsApostropheDifference(t *testing.T) {
	results := []tvmazeSearchResult{{
		Score: 1,
		Show:  tvmazeShow{Name: "Grey's Anatomy"},
	}}
	got := selectBestTVMazeShow(results, "Greys Anatomy")
	if got == nil || got.Name != "Grey's Anatomy" {
		t.Fatalf("selectBestTVMazeShow() = %#v, want apostrophe-normalized match", got)
	}
}
