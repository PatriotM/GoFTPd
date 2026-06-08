package imdb

import "testing"

func TestParseMovieName(t *testing.T) {
	title, year := parseMovieName("Daemonen.1986.REMASTERED.German.720p.BluRay.x264-CONTRiBUTiON")
	if title != "Daemonen" || year != 1986 {
		t.Fatalf("parseMovieName() = %q, %d; want Daemonen, 1986", title, year)
	}
}

func TestSelectBestIMDBTitleRejectsYearMismatch(t *testing.T) {
	titles := []imdbTitle{{
		ID:           "tt1457767",
		Type:         "movie",
		PrimaryTitle: "The Conjuring",
		StartYear:    2013,
	}}
	if got := selectBestIMDBTitle(titles, "Daemonen", 1986); got != nil {
		t.Fatalf("selectBestIMDBTitle() = %#v, want nil for unsafe year mismatch", got)
	}
}

func TestSelectBestIMDBTitleAcceptsLocalizedExactYear(t *testing.T) {
	titles := []imdbTitle{{
		ID:           "tt0000001",
		Type:         "movie",
		PrimaryTitle: "Dämonen",
		StartYear:    1986,
	}}
	got := selectBestIMDBTitle(titles, "Daemonen", 1986)
	if got == nil || got.ID != "tt0000001" {
		t.Fatalf("selectBestIMDBTitle() = %#v, want localized exact-year match", got)
	}
}
