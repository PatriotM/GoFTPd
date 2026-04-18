package plugin

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

	"goftpd/sitebot/internal/event"
	tmpl "goftpd/sitebot/internal/template"
)

type TVMazePlugin struct {
	debug    bool
	theme    *tmpl.Theme
	mu       sync.Mutex
	seen     map[string]bool
	client   *http.Client
	sections []string // only announce when section contains one of these (case-insensitive)
}

type tvmazeShow struct {
	Name     string   `json:"name"`
	Language string   `json:"language"`
	Genres   []string `json:"genres"`
	Type     string   `json:"type"`
	Rating   struct {
		Average float64 `json:"average"`
	} `json:"rating"`
	Network struct {
		Name string `json:"name"`
	} `json:"network"`
	WebChannel struct {
		Name string `json:"name"`
	} `json:"webChannel"`
	URL string `json:"url"`
}

type tvmazeSearchResult struct {
	Show tvmazeShow `json:"show"`
}

func NewTVMazePlugin() *TVMazePlugin {
	return &TVMazePlugin{
		seen:     map[string]bool{},
		client:   &http.Client{Timeout: 8 * time.Second},
		sections: []string{"TV"},
	}
}

func (p *TVMazePlugin) Name() string { return "TVMaze" }

func (p *TVMazePlugin) Initialize(config map[string]interface{}) error {
	if debug, ok := config["debug"].(bool); ok {
		p.debug = debug
	}
	if themeFile, ok := config["theme_file"].(string); ok && strings.TrimSpace(themeFile) != "" {
		th, err := tmpl.LoadTheme(themeFile)
		if err == nil {
			p.theme = th
		}
	}
	if secs, ok := config["tvmaze_sections"].([]string); ok && len(secs) > 0 {
		p.sections = secs
	}
	return nil
}

func (p *TVMazePlugin) Close() error { return nil }

// extractShowName parses a scene-style release name and returns a query title.
// e.g. "Fire.Country.S04E15.1080p.WEB.h264-ETHEL" -> "Fire Country"
func extractShowName(rel string) string {
	if idx := strings.LastIndex(rel, "-"); idx > 0 {
		rel = rel[:idx]
	}
	re := regexp.MustCompile(`(?i)\.(S\d{1,2}(E\d{1,3})?|Season\.?\d+|\d{4})\.`)
	if loc := re.FindStringIndex(rel); loc != nil {
		rel = rel[:loc[0]]
	}
	return strings.ReplaceAll(rel, ".", " ")
}

func (p *TVMazePlugin) sectionMatches(section string) bool {
	up := strings.ToUpper(section)
	for _, s := range p.sections {
		if strings.Contains(up, strings.ToUpper(s)) {
			return true
		}
	}
	return false
}

func (p *TVMazePlugin) lookup(query string) (*tvmazeShow, error) {
	u := "https://api.tvmaze.com/search/shows?q=" + url.QueryEscape(query)
	resp, err := p.client.Get(u)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("tvmaze status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var results []tvmazeSearchResult
	if err := json.Unmarshal(body, &results); err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no results")
	}
	s := results[0].Show
	return &s, nil
}

func (p *TVMazePlugin) OnEvent(evt *event.Event) ([]Output, error) {
	if evt.Type != event.EventMKDir {
		return nil, nil
	}
	if !p.sectionMatches(evt.Section) {
		return nil, nil
	}
	rel := path.Base(path.Clean(evt.Path))
	if rel == "" || rel == "." || rel == "/" {
		return nil, nil
	}

	p.mu.Lock()
	if p.seen[rel] {
		p.mu.Unlock()
		return nil, nil
	}
	p.seen[rel] = true
	if len(p.seen) > 2000 {
		p.seen = map[string]bool{rel: true}
	}
	p.mu.Unlock()

	query := extractShowName(rel)
	if query == "" {
		return nil, nil
	}
	show, err := p.lookup(query)
	if err != nil {
		if p.debug {
			log.Printf("[TVMaze] lookup %q failed: %v", query, err)
		}
		return nil, nil
	}

	genres := "N/A"
	if len(show.Genres) > 0 {
		genres = strings.Join(show.Genres, ", ")
	}
	network := show.Network.Name
	if network == "" {
		network = show.WebChannel.Name
	}
	if network == "" {
		network = "N/A"
	}
	showType := show.Type
	if showType == "" {
		showType = "N/A"
	}
	language := show.Language
	if language == "" {
		language = "N/A"
	}
	rating := "N/A"
	if show.Rating.Average > 0 {
		rating = fmt.Sprintf("%.1f", show.Rating.Average)
	}
	link := show.URL
	if link == "" {
		link = "N/A"
	}

	vars := map[string]string{
		"section":  evt.Section,
		"relname":  rel,
		"genre":    genres,
		"type":     showType,
		"network":  network,
		"rating":   rating,
		"language": language,
		"link":     link,
		"title":    show.Name,
	}
	for k, v := range evt.Data {
		vars[k] = v
	}

	var text string
	if p.theme != nil {
		if raw, ok := p.theme.Announces["TVINFO"]; ok && raw != "" {
			text = tmpl.Render(raw, vars)
		}
	}
	if text == "" {
		text = fmt.Sprintf("TV-INFO: [%s] %s Genre: %s - Type: %s - Link: %s - Network: %s - Rating: %s - Language: %s",
			evt.Section, rel, genres, showType, link, network, rating, language)
	}
	return []Output{{Type: "TV-INFO", Text: text}}, nil
}
