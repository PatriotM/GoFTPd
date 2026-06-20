package tvmaze

import "testing"

func TestParseTVNameStopsAtYearTag(t *testing.T) {
	title, season, episode := parseTVName("The.Price.Is.Right.2026.06.01.1080p.WEB.h264-DiRT")
	if title != "The Price Is Right" || season != 0 || episode != 0 {
		t.Fatalf("parseTVName() = %q, %d, %d; want The Price Is Right, 0, 0", title, season, episode)
	}
}

func TestParseTVNameStripsYearBeforeSeason(t *testing.T) {
	title, yearHint, season, episode := parseTVNameWithYear("Ghosts.2021.S05E01.1080p.WEB.h264-GROUP")
	if title != "Ghosts" || yearHint != 2021 || season != 5 || episode != 1 {
		t.Fatalf("parseTVNameWithYear() = %q, %d, %d, %d; want Ghosts, 2021, 5, 1", title, yearHint, season, episode)
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

func TestSelectBestTVMazeShowAcceptsPossessiveTitleVariant(t *testing.T) {
	results := []tvmSearchResult{{
		Score: 1,
		Show:  tvmShow{Name: "Bob's Burgers"},
	}}
	got := selectBestTVMazeShow(results, "Bobs Burgers")
	if got == nil || got.Name != "Bob's Burgers" {
		t.Fatalf("selectBestTVMazeShow() = %#v, want Bob's Burgers", got)
	}
}

func TestSelectBestTVMazeShowSplitsLetterDigitTitles(t *testing.T) {
	results := []tvmSearchResult{{
		Score: 1,
		Show:  tvmShow{Name: "Formula 1"},
	}}
	got := selectBestTVMazeShow(results, "Formula1")
	if got == nil || got.Name != "Formula 1" {
		t.Fatalf("selectBestTVMazeShow() = %#v, want Formula 1", got)
	}
}

func TestSelectBestTVMazeShowPrefersPremieredYear(t *testing.T) {
	results := []tvmSearchResult{
		{Score: 1, Show: tvmShow{Name: "Ghosts", Premiered: "2019-04-15"}},
		{Score: 1, Show: tvmShow{Name: "Ghosts", Premiered: "2021-10-07"}},
	}
	got := selectBestTVMazeShow(results, "Ghosts", 2021)
	if got == nil || got.Premiered != "2021-10-07" {
		t.Fatalf("selectBestTVMazeShow() = %#v, want 2021 Ghosts", got)
	}
}

func TestTVMazeLookupQueriesAddsLoveIslandUSAlias(t *testing.T) {
	got := tvmazeLookupQueries("Love Island US")
	if len(got) != 2 || got[0] != "Love Island US" || got[1] != "Love Island USA" {
		t.Fatalf("tvmazeLookupQueries() = %#v, want Love Island US then Love Island USA", got)
	}
}
