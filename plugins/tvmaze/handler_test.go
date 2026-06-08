package tvmaze

import "testing"

func TestParseTVNameStopsAtYearTag(t *testing.T) {
	title, season, episode := parseTVName("The.Price.Is.Right.2026.06.01.1080p.WEB.h264-DiRT")
	if title != "The Price Is Right" || season != 0 || episode != 0 {
		t.Fatalf("parseTVName() = %q, %d, %d; want The Price Is Right, 0, 0", title, season, episode)
	}
}

func TestParseTVNameDropsShowYearBeforeSeason(t *testing.T) {
	title, season, episode := parseTVName("Fallout.2024.S02E01.MULTi.2160p.UHD.BluRay.x265-SODAPOP")
	if title != "Fallout" || season != 2 || episode != 1 {
		t.Fatalf("parseTVName() = %q, %d, %d; want Fallout, 2, 1", title, season, episode)
	}
}

func TestParseTVMazeReleaseQueryKeepsShowYearFilter(t *testing.T) {
	query := parseTVMazeReleaseQuery("Fallout.2024.S02E01.MULTi.2160p.UHD.BluRay.x265-SODAPOP")
	if query.Title != "Fallout" || query.Year != "2024" || query.Season != 2 || query.Episode != 1 {
		t.Fatalf("parseTVMazeReleaseQuery() = %#v; want title Fallout, year 2024, S02E01", query)
	}
}

func TestSelectBestTVMazeShowFollowsSearchOrder(t *testing.T) {
	results := []tvmSearchResult{{
		Score: 1,
		Show:  tvmShow{Name: "The Walking Dead"},
	}}
	if got := selectBestTVMazeShow(results, "The Price Is Right"); got == nil || got.Name != "The Walking Dead" {
		t.Fatalf("selectBestTVMazeShow() = %#v, want first search result", got)
	}
}

func TestSelectBestTVMazeShowAppliesYearFilter(t *testing.T) {
	results := []tvmSearchResult{{
		Score: 1,
		Show:  tvmShow{Name: "Fallout", Premiered: "1997-09-14"},
	}, {
		Score: 1,
		Show:  tvmShow{Name: "Fallout", Premiered: "2024-04-10"},
	}}
	got := selectBestTVMazeShow(results, "Fallout", tvmazeMatchCriteria{Year: "2024"})
	if got == nil || got.Premiered != "2024-04-10" {
		t.Fatalf("selectBestTVMazeShow() = %#v, want 2024 match", got)
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

func TestSelectBestTVMazeShowAcceptsApostropheDifference(t *testing.T) {
	results := []tvmSearchResult{{
		Score: 1,
		Show:  tvmShow{Name: "Grey's Anatomy"},
	}}
	got := selectBestTVMazeShow(results, "Greys Anatomy")
	if got == nil || got.Name != "Grey's Anatomy" {
		t.Fatalf("selectBestTVMazeShow() = %#v, want apostrophe-normalized match", got)
	}
}
