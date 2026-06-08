// Package tvmaze is a goftpd plugin that looks up TV show metadata on
// TVMaze when a release directory is created, and writes a .tvmaze file
// into the release dir for display via show_diz.
//
// Config (in the main goftpd config.yml under plugins.tvmaze):
//
//	plugins:
//	  tvmaze:
//	    enabled: true
//	    sections: ["TV"]        # section substring match (case-insensitive)
//	    debug: false
package tvmaze

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"goftpd/internal/plugin"
)

// Handler is the tvmaze plugin. One instance per goftpd process.
type Handler struct {
	svc      *plugin.Services
	debug    bool
	sections []string
	version  string
	proxy    string

	client   *http.Client
	jobs     chan job
	seen     map[string]bool
	seenMu   sync.Mutex
	stopCh   chan struct{}
	stopOnce sync.Once
}

type job struct {
	dirPath string
	relname string
	section string
}

type tvmazeReleaseQuery struct {
	Title       string
	Season      int
	Episode     int
	Year        string
	CountryCode string
}

// New returns an uninitialised Handler. Init() wires it up.
func New() *Handler {
	return &Handler{
		client: &http.Client{Timeout: 10 * time.Second},
		jobs:   make(chan job, 128),
		seen:   make(map[string]bool),
		stopCh: make(chan struct{}),
	}
}

// Name satisfies plugin.Plugin.
func (h *Handler) Name() string { return "tvmaze" }

// Init wires up services + config and starts the background worker.
func (h *Handler) Init(svc *plugin.Services, cfg map[string]interface{}) error {
	h.svc = svc
	h.sections = toStringSlice(cfg["sections"])
	if len(h.sections) == 0 {
		h.sections = []string{"TV"}
	}
	if v, ok := cfg["debug"].(bool); ok {
		h.debug = v
	} else if svc != nil {
		h.debug = svc.Debug
	}
	if v, ok := cfg["version"].(string); ok && strings.TrimSpace(v) != "" {
		h.version = strings.TrimSpace(v)
	} else {
		h.version = "1.0"
	}
	h.proxy = stringConfig(cfg, "proxy", "")
	client, err := newHTTPClient(10*time.Second, h.proxy)
	if err != nil {
		return err
	}
	h.client = client
	go h.worker()
	if h.debug {
		log.Printf("[TVMAZE] initialized, sections=%v proxy=%q", h.sections, h.proxy)
	}
	return nil
}

// OnEvent is the hot path — must return fast. We only care about MKDIR.
func (h *Handler) OnEvent(evt *plugin.Event) error {
	if evt == nil || evt.Type != plugin.EventMKDir {
		return nil
	}
	if h.debug {
		log.Printf("[TVMAZE] OnEvent MKDIR path=%s filename=%s section=%s", evt.Path, evt.Filename, evt.Section)
	}
	if h.svc == nil || h.svc.Bridge == nil {
		log.Printf("[TVMAZE] skipping %s: svc or bridge nil", evt.Filename)
		return nil
	}
	if !matchSection(evt.Section, h.sections) {
		if h.debug {
			log.Printf("[TVMAZE] skipping %s: section %q not in %v", evt.Filename, evt.Section, h.sections)
		}
		return nil
	}
	if !isTVReleaseName(evt.Filename) {
		if h.debug {
			log.Printf("[TVMAZE] skipping %s: not a TV release name", evt.Filename)
		}
		return nil
	}

	// Dedupe by path — avoids double-lookup if the same dir is re-created.
	h.seenMu.Lock()
	if h.seen[evt.Path] {
		h.seenMu.Unlock()
		return nil
	}
	h.seen[evt.Path] = true
	if len(h.seen) > 2000 {
		h.seen = make(map[string]bool)
	}
	h.seenMu.Unlock()

	log.Printf("[TVMAZE] queued lookup for %s", evt.Filename)

	select {
	case h.jobs <- job{dirPath: evt.Path, relname: evt.Filename, section: evt.Section}:
	default:
		log.Printf("[TVMAZE] job queue full, dropping %s", evt.Filename)
	}
	return nil
}

// Stop closes the worker.
func (h *Handler) Stop() error {
	h.stopOnce.Do(func() { close(h.stopCh) })
	return nil
}

// worker drains the job queue sequentially (TVMaze has a request/second limit).
func (h *Handler) worker() {
	for {
		select {
		case <-h.stopCh:
			return
		case j := <-h.jobs:
			h.doLookup(j)
		}
	}
}

// =============================================================================
// TVMaze API
// =============================================================================

type tvmShow struct {
	ID        int      `json:"id"`
	Name      string   `json:"name"`
	URL       string   `json:"url"`
	Language  string   `json:"language"`
	Genres    []string `json:"genres"`
	Type      string   `json:"type"`
	Status    string   `json:"status"`
	Premiered string   `json:"premiered"`
	Rating    struct {
		Average float64 `json:"average"`
	} `json:"rating"`
	Network struct {
		Name    string `json:"name"`
		Country struct {
			Code string `json:"code"`
			Name string `json:"name"`
		} `json:"country"`
	} `json:"network"`
	WebChannel struct {
		Name    string `json:"name"`
		Country struct {
			Code string `json:"code"`
		} `json:"country"`
	} `json:"webChannel"`
	Summary   string `json:"summary"`
	Externals struct {
		IMDB string `json:"imdb"`
	} `json:"externals"`
	Embedded struct {
		Episodes []tvmEpisode `json:"episodes"`
	} `json:"_embedded"`
}

type tvmEpisode struct {
	ID      int    `json:"id"`
	Name    string `json:"name"`
	Season  int    `json:"season"`
	Number  int    `json:"number"`
	Airdate string `json:"airdate"`
	URL     string `json:"url"`
	Summary string `json:"summary"`
}

type tvmSearchResult struct {
	Score float64 `json:"score"`
	Show  tvmShow `json:"show"`
}

func (h *Handler) doLookup(j job) {
	query := parseTVMazeReleaseQuery(j.relname)
	if query.Title == "" {
		log.Printf("[TVMAZE] parseTVName returned empty title for %s, skipping", j.relname)
		return
	}
	log.Printf("[TVMAZE] lookup %s (title=%q season=%d episode=%d year=%q country=%q)", j.relname, query.Title, query.Season, query.Episode, query.Year, query.CountryCode)

	show, err := h.lookupShow(query.Title, query.Year, query.CountryCode)
	if err != nil {
		log.Printf("[TVMAZE] search %q failed: %v", query.Title, err)
		return
	}

	if show.ID > 0 {
		if full, err := h.fetchShowWithEpisodes(show.ID); err == nil && full.ID > 0 {
			show = full
		} else if h.debug {
			log.Printf("[TVMAZE] episode fetch %q failed: %v", query.Title, err)
		}
	}

	var ep *tvmEpisode
	if query.Season > 0 && query.Episode > 0 {
		for i := range show.Embedded.Episodes {
			e := &show.Embedded.Episodes[i]
			if e.Season == query.Season && e.Number == query.Episode {
				ep = e
				break
			}
		}
	}

	content := formatTVMazeFile(show, ep, h.version)
	filePath := path.Join(j.dirPath, ".tvmaze")
	if err := h.svc.Bridge.WriteFile(filePath, []byte(content)); err != nil {
		log.Printf("[TVMAZE] WriteFile %s failed: %v", filePath, err)
		return
	}
	log.Printf("[TVMAZE] Wrote .tvmaze for %s", j.relname)
}

func (h *Handler) lookupShow(title, year, countryCode string) (*tvmShow, error) {
	q := url.QueryEscape(title)
	searchURL := fmt.Sprintf("https://api.tvmaze.com/search/shows?q=%s", q)
	resp, err := h.client.Get(searchURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var results []tvmSearchResult
	if err := json.Unmarshal(body, &results); err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no results")
	}
	show := selectBestTVMazeShow(results, title, tvmazeMatchCriteria{Year: year, CountryCode: countryCode})
	if show == nil {
		return nil, fmt.Errorf("no show matched search criteria")
	}
	return show, nil
}

func (h *Handler) fetchShowWithEpisodes(id int) (*tvmShow, error) {
	u := fmt.Sprintf("https://api.tvmaze.com/shows/%d?embed=episodes", id)
	resp, err := h.client.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var show tvmShow
	if err := json.Unmarshal(body, &show); err != nil {
		return nil, err
	}
	return &show, nil
}

// =============================================================================
// Parsing / formatting
// =============================================================================

// isTVReleaseName: scene TV naming — dots, dash, and SxxEyy or year tag.
func isTVReleaseName(rel string) bool {
	if !strings.Contains(rel, ".") || !strings.Contains(rel, "-") {
		return false
	}
	re := regexp.MustCompile(`(?i)(^|\.)(S\d{1,2}(E\d{1,3})?|Season\.?\d+|\d{4})(\.|$)`)
	return re.MatchString(rel)
}

// parseTVName: "Kill.Blue.S01E02.1080p.WEB.H264-SKYANiME" -> ("Kill Blue", 1, 2)
func parseTVName(rel string) (string, int, int) {
	query := parseTVMazeReleaseQuery(rel)
	return query.Title, query.Season, query.Episode
}

func parseTVMazeReleaseQuery(rel string) tvmazeReleaseQuery {
	if idx := strings.LastIndex(rel, "-"); idx > 0 {
		rel = rel[:idx]
	}
	query := tvmazeReleaseQuery{}
	titlePart := rel
	if loc := regexp.MustCompile(`(?i)(^|[.\s])(S(\d+)\.?(?:E(\d+))?)`).FindStringSubmatchIndex(rel); loc != nil {
		titlePart = strings.TrimRight(rel[:loc[2]], ". ")
		fmt.Sscanf(rel[loc[6]:loc[7]], "%d", &query.Season)
		if loc[8] >= 0 && loc[9] >= 0 {
			fmt.Sscanf(rel[loc[8]:loc[9]], "%d", &query.Episode)
		}
	} else if loc := regexp.MustCompile(`(?i)(^|[.\s])((\d+)x(\d+))`).FindStringSubmatchIndex(rel); loc != nil {
		titlePart = strings.TrimRight(rel[:loc[2]], ". ")
		fmt.Sscanf(rel[loc[6]:loc[7]], "%d", &query.Season)
		fmt.Sscanf(rel[loc[8]:loc[9]], "%d", &query.Episode)
	} else if loc := regexp.MustCompile(`(?i)(^|[.\s])(Season\.?\d+|(?:19|20)\d{2})([.\s]|$)`).FindStringSubmatchIndex(rel); loc != nil {
		titlePart = strings.TrimRight(rel[:loc[2]], ". ")
	}

	filterCut := len(titlePart)
	if loc := regexp.MustCompile(`(?i)(^|[.\s])((?:19|20)\d{2})([.\s]|$)`).FindStringSubmatchIndex(titlePart); loc != nil {
		query.Year = titlePart[loc[4]:loc[5]]
		filterCut = min(filterCut, loc[4])
	}
	if loc := regexp.MustCompile(`(?i)(^|[.\s])(UK|GB|US|CA|AU)([.\s]|$)`).FindStringSubmatchIndex(titlePart); loc != nil {
		query.CountryCode = strings.ToUpper(titlePart[loc[4]:loc[5]])
		if query.CountryCode == "UK" {
			query.CountryCode = "GB"
		}
		filterCut = min(filterCut, loc[4])
	}
	if filterCut < len(titlePart) {
		titlePart = strings.TrimRight(titlePart[:filterCut], ". ")
	}
	query.Title = strings.Join(strings.Fields(strings.ReplaceAll(titlePart, ".", " ")), " ")
	return query
}

type tvmazeMatchCriteria struct {
	Year        string
	CountryCode string
}

func selectBestTVMazeShow(results []tvmSearchResult, query string, criteriaList ...tvmazeMatchCriteria) *tvmShow {
	criteria := tvmazeMatchCriteria{}
	if len(criteriaList) > 0 {
		criteria = criteriaList[0]
	}
	for i := range results {
		show := &results[i].Show
		if criteria.Year != "" && !strings.HasPrefix(show.Premiered, criteria.Year) {
			continue
		}
		if criteria.CountryCode != "" && !strings.EqualFold(showCountryCode(show), criteria.CountryCode) {
			continue
		}
		return show
	}
	return nil
}

func showCountryCode(show *tvmShow) string {
	if show == nil {
		return ""
	}
	if show.Network.Country.Code != "" {
		return show.Network.Country.Code
	}
	return show.WebChannel.Country.Code
}

func titleSimilarityScore(query, candidate string) int {
	q := normalizeLookupTitle(query)
	c := normalizeLookupTitle(candidate)
	if q == "" || c == "" {
		return 0
	}
	if q == c {
		return 100
	}
	if strings.Contains(c, q) || strings.Contains(q, c) {
		return 85
	}

	qTokens := strings.Fields(q)
	cTokens := strings.Fields(c)
	if len(qTokens) == 0 || len(cTokens) == 0 {
		return 0
	}
	cSet := make(map[string]struct{}, len(cTokens))
	for _, token := range cTokens {
		cSet[token] = struct{}{}
	}
	cInitials := tokenInitials(cTokens)
	common := 0
	for _, token := range qTokens {
		if _, ok := cSet[token]; ok {
			common++
			continue
		}
		if len(token) >= 2 && strings.Contains(cInitials, token) {
			common++
		}
	}
	queryCoverage := common * 100 / len(qTokens)
	if queryCoverage >= 90 && common >= 2 {
		return 90
	}
	maxTokens := len(qTokens)
	if len(cTokens) > maxTokens {
		maxTokens = len(cTokens)
	}
	return common * 100 / maxTokens
}

func tokenInitials(tokens []string) string {
	var b strings.Builder
	for _, token := range tokens {
		if token != "" {
			b.WriteByte(token[0])
		}
	}
	return b.String()
}

var lookupTitleReplacer = strings.NewReplacer(
	"ä", "ae", "ö", "oe", "ü", "ue", "ß", "ss",
	"à", "a", "á", "a", "â", "a", "ã", "a", "å", "a",
	"è", "e", "é", "e", "ê", "e", "ë", "e",
	"ì", "i", "í", "i", "î", "i", "ï", "i",
	"ò", "o", "ó", "o", "ô", "o", "õ", "o",
	"ù", "u", "ú", "u", "û", "u",
	"ç", "c", "ñ", "n",
	"&", " and ",
)

func normalizeLookupTitle(s string) string {
	s = lookupTitleReplacer.Replace(strings.ToLower(strings.TrimSpace(s)))
	s = strings.NewReplacer("'", "", "\u2019", "", "`", "").Replace(s)
	var b strings.Builder
	lastSpace := false
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastSpace = false
			continue
		}
		if !lastSpace {
			b.WriteByte(' ')
			lastSpace = true
		}
	}
	return strings.Join(strings.Fields(b.String()), " ")
}

func formatTVMazeFile(show *tvmShow, ep *tvmEpisode, version string) string {
	var b strings.Builder
	bar := fmt.Sprintf("======================== TVMAZE INFO v%s ========================", version)
	fmt.Fprintf(&b, "%s\n\n", bar)

	fmt.Fprintf(&b, " Title........: %s\n", show.Name)
	if ep != nil {
		fmt.Fprintf(&b, " Episode......: S%02dE%02d - %s\n", ep.Season, ep.Number, ep.Name)
	}
	premiered := show.Premiered
	if len(premiered) >= 4 {
		premiered = premiered[:4]
	}
	if premiered != "" {
		fmt.Fprintf(&b, " Premiered....: %s\n", premiered)
	}
	fmt.Fprintf(&b, " -\n")

	if show.Externals.IMDB != "" {
		fmt.Fprintf(&b, " IMDB Link....: https://www.imdb.com/title/%s/\n", show.Externals.IMDB)
	} else {
		fmt.Fprintf(&b, " IMDB Link....: NA\n")
	}
	if show.URL != "" {
		fmt.Fprintf(&b, " TVMaze Link..: %s\n", show.URL)
	}
	if ep != nil && ep.URL != "" {
		fmt.Fprintf(&b, " Episode Link.: %s\n", ep.URL)
	}
	if len(show.Genres) > 0 {
		fmt.Fprintf(&b, " Genre........: %s\n", strings.Join(show.Genres, ", "))
	}
	if show.Type != "" {
		fmt.Fprintf(&b, " Type.........: %s\n", show.Type)
	}
	rating := "NA"
	if show.Rating.Average > 0 {
		rating = fmt.Sprintf("%.1f", show.Rating.Average)
	}
	fmt.Fprintf(&b, " User Rating..: %s\n", rating)
	fmt.Fprintf(&b, " -\n")

	country := show.Network.Country.Code
	if country == "" {
		country = "NA"
	}
	fmt.Fprintf(&b, " Country......: %s\n", country)
	language := show.Language
	if language == "" {
		language = "NA"
	}
	fmt.Fprintf(&b, " Language.....: %s\n", language)
	network := show.Network.Name
	if network == "" {
		network = show.WebChannel.Name
	}
	if network == "" {
		network = "NA"
	}
	fmt.Fprintf(&b, " Network......: %s\n", network)
	status := show.Status
	if status == "" {
		status = "NA"
	}
	fmt.Fprintf(&b, " Status.......: %s\n", status)
	if ep != nil && ep.Airdate != "" {
		fmt.Fprintf(&b, " Airdate......: %s\n", ep.Airdate)
	}
	fmt.Fprintf(&b, " -\n")

	plot := ""
	if ep != nil && ep.Summary != "" {
		plot = ep.Summary
	} else {
		plot = show.Summary
	}
	plot = stripHTML(plot)
	if plot == "" {
		plot = "NA"
	}
	fmt.Fprintf(&b, " Plot.........: %s\n", wrapPlot(plot, 70))
	fmt.Fprintf(&b, "\n%s\n", bar)
	return b.String()
}

// =============================================================================
// Helpers (kept local so each plugin is self-contained)
// =============================================================================

func matchSection(section string, allowed []string) bool {
	if section == "" || len(allowed) == 0 {
		return false
	}
	up := strings.ToUpper(section)
	for _, s := range allowed {
		if strings.Contains(up, strings.ToUpper(s)) {
			return true
		}
	}
	return false
}

func stripHTML(s string) string {
	s = regexp.MustCompile(`<[^>]+>`).ReplaceAllString(s, "")
	s = strings.ReplaceAll(s, "&amp;", "&")
	s = strings.ReplaceAll(s, "&quot;", "\"")
	s = strings.ReplaceAll(s, "&#39;", "'")
	s = strings.ReplaceAll(s, "&lt;", "<")
	s = strings.ReplaceAll(s, "&gt;", ">")
	return strings.TrimSpace(s)
}

func wrapPlot(s string, width int) string {
	s = strings.TrimSpace(s)
	if len(s) <= width {
		return s
	}
	var out strings.Builder
	words := strings.Fields(s)
	line := ""
	first := true
	for _, w := range words {
		if len(line)+1+len(w) > width {
			if first {
				out.WriteString(line)
				first = false
			} else {
				out.WriteString("\n                ")
				out.WriteString(line)
			}
			line = w
			continue
		}
		if line == "" {
			line = w
		} else {
			line += " " + w
		}
	}
	if line != "" {
		if first {
			out.WriteString(line)
		} else {
			out.WriteString("\n                ")
			out.WriteString(line)
		}
	}
	return out.String()
}

// toStringSlice normalizes yaml-parsed slices which can come back as
// []interface{} or a single string.
func toStringSlice(v interface{}) []string {
	if v == nil {
		return nil
	}
	switch s := v.(type) {
	case []string:
		return s
	case []interface{}:
		out := make([]string, 0, len(s))
		for _, x := range s {
			if str, ok := x.(string); ok {
				out = append(out, str)
			}
		}
		return out
	case string:
		if s == "" {
			return nil
		}
		parts := strings.Split(s, ",")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		return parts
	}
	return nil
}

func stringConfig(cfg map[string]interface{}, key, fallback string) string {
	if cfg == nil {
		return fallback
	}
	if s, ok := cfg[key].(string); ok {
		return strings.TrimSpace(s)
	}
	return fallback
}

func newHTTPClient(timeout time.Duration, proxyValue string) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	proxyValue = strings.TrimSpace(proxyValue)
	if proxyValue != "" {
		proxyURL, err := url.Parse(proxyValue)
		if err != nil {
			return nil, fmt.Errorf("tvmaze proxy %q is invalid: %w", proxyValue, err)
		}
		transport.Proxy = http.ProxyURL(proxyURL)
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}, nil
}
