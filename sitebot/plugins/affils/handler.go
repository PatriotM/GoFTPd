package affils

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"goftpd/sitebot/internal/event"
	"goftpd/sitebot/internal/plugin"
	tmpl "goftpd/sitebot/internal/template"
	"gopkg.in/yaml.v3"
)

type Plugin struct {
	affils      []Affil
	file        string
	replyTarget string
	showPredirs bool
	theme       *tmpl.Theme
}

type Affil struct {
	Group       string                 `yaml:"group"`
	Predir      string                 `yaml:"predir"`
	Permissions map[string]interface{} `yaml:"permissions"`
}

type affilsFileConfig struct {
	Base   string  `yaml:"base"`
	Groups []Affil `yaml:"groups"`
}

func New() *Plugin {
	return &Plugin{
		file:        "etc/affils.yml",
		replyTarget: "channel",
	}
}

func (p *Plugin) Name() string { return "Affils" }

func (p *Plugin) Initialize(config map[string]interface{}) error {
	if themeFile, ok := config["theme_file"].(string); ok && strings.TrimSpace(themeFile) != "" {
		th, err := tmpl.LoadTheme(themeFile)
		if err == nil {
			p.theme = th
		}
	}
	cfg := plugin.ConfigSection(config, "affils")
	if s, ok := stringConfig(cfg, config, "reply_target", "affils_reply_target"); ok && strings.TrimSpace(s) != "" {
		p.replyTarget = strings.ToLower(strings.TrimSpace(s))
	}
	if s, ok := stringConfig(cfg, config, "file", "affils_file"); ok && strings.TrimSpace(s) != "" {
		p.file = strings.TrimSpace(s)
	}
	if b, ok := boolConfig(configValue(cfg, config, "show_predirs", "affils_show_predirs")); ok {
		p.showPredirs = b
	}
	p.affils = sortedAffils(affilsConfig(configValue(cfg, config, "groups", "affils")))
	return nil
}

func (p *Plugin) Close() error { return nil }

func (p *Plugin) OnEvent(evt *event.Event) ([]plugin.Output, error) {
	if evt.Type != event.EventCommand {
		return nil, nil
	}
	cmd := strings.ToLower(strings.TrimSpace(evt.Data["command"]))
	switch cmd {
	case "affils", "affil":
		return p.showAffils(evt), nil
	case "addaffil":
		return p.addAffil(evt), nil
	case "delaffil":
		return p.delAffil(evt), nil
	case "pre":
		return p.reply(evt, p.render("AFFILS_PRE_HELP", nil, "PRE: use SITE PRE <releasename> <section> in FTP.")), nil
	default:
		return nil, nil
	}
}

func (p *Plugin) showAffils(evt *event.Event) []plugin.Output {
	affils := p.currentAffils()
	if len(affils) == 0 {
		return p.reply(evt, p.render("AFFILS_EMPTY", nil, "AFFILS: No affils configured."))
	}
	if p.showPredirs {
		lines := make([]string, 0, len(affils))
		for _, affil := range affils {
			lines = append(lines, p.render("AFFIL_ENTRY", affilVars(affil), fmt.Sprintf("AFFIL: %s - %s", affil.Group, affil.Predir)))
		}
		return p.replies(evt, lines...)
	}
	groups := make([]string, 0, len(affils))
	for _, affil := range affils {
		groups = append(groups, affil.Group)
	}
	return p.reply(evt, p.render("AFFILS_LIST", map[string]string{"groups": strings.Join(groups, ", ")}, "AFFILS: "+strings.Join(groups, ", ")))
}

func (p *Plugin) addAffil(evt *event.Event) []plugin.Output {
	fields := strings.Fields(strings.TrimSpace(evt.Data["args"]))
	if len(fields) < 1 {
		return p.reply(evt, p.render("AFFILS_USAGE_ADD", nil, "AFFILS: Usage: !addaffil <group> [predir]"))
	}
	group := strings.TrimSpace(fields[0])
	if !validAffilGroup(group) {
		return p.reply(evt, p.render("AFFILS_INVALID", map[string]string{"group": group}, fmt.Sprintf("AFFILS: Invalid affil group %s.", group)))
	}

	cfg := p.currentAffilsFileConfig()
	for _, affil := range cfg.Groups {
		if strings.EqualFold(affil.Group, group) {
			return p.reply(evt, p.render("AFFILS_EXISTS", affilVars(affil), fmt.Sprintf("AFFILS: %s already exists.", affil.Group)))
		}
	}

	predir := path.Join(cleanAbs(cfg.Base), group)
	if len(fields) > 1 && strings.TrimSpace(fields[1]) != "" {
		predir = cleanAbs(fields[1])
	}
	affil := Affil{
		Group:  group,
		Predir: predir,
		Permissions: map[string]interface{}{
			"privpath":    "/site" + predir,
			"owner_group": group,
			"mode":        "0777",
		},
	}
	cfg.Groups = append(cfg.Groups, affil)
	cfg.Groups = sortedAffils(cfg.Groups)

	if err := p.saveAffilsFileConfig(cfg); err != nil {
		return p.reply(evt, p.render("AFFILS_ERROR", map[string]string{"error": err.Error()}, fmt.Sprintf("AFFILS: Could not update %s: %v", p.file, err)))
	}
	return p.reply(evt, p.render("AFFILS_ADDED", affilVars(affil), fmt.Sprintf("AFFILS: Added %s with predir %s.", group, predir)))
}

func (p *Plugin) delAffil(evt *event.Event) []plugin.Output {
	fields := strings.Fields(strings.TrimSpace(evt.Data["args"]))
	if len(fields) < 1 {
		return p.reply(evt, p.render("AFFILS_USAGE_DEL", nil, "AFFILS: Usage: !delaffil <group>"))
	}
	group := strings.TrimSpace(fields[0])

	cfg := p.currentAffilsFileConfig()
	kept := make([]Affil, 0, len(cfg.Groups))
	var removed *Affil
	for _, affil := range cfg.Groups {
		if strings.EqualFold(affil.Group, group) {
			copy := affil
			removed = &copy
			continue
		}
		kept = append(kept, affil)
	}
	if removed == nil {
		return p.reply(evt, p.render("AFFILS_NOTFOUND", map[string]string{"group": group}, fmt.Sprintf("AFFILS: %s not found.", group)))
	}
	cfg.Groups = kept
	if err := p.saveAffilsFileConfig(cfg); err != nil {
		return p.reply(evt, p.render("AFFILS_ERROR", map[string]string{"error": err.Error()}, fmt.Sprintf("AFFILS: Could not update %s: %v", p.file, err)))
	}
	return p.reply(evt, p.render("AFFILS_REMOVED", affilVars(*removed), fmt.Sprintf("AFFILS: Removed %s. Predir %s was left on disk.", removed.Group, removed.Predir)))
}

func (p *Plugin) currentAffils() []Affil {
	return p.currentAffilsFileConfig().Groups
}

func (p *Plugin) currentAffilsFileConfig() affilsFileConfig {
	cfg, err := loadAffilsFile(p.file)
	if err == nil {
		if strings.TrimSpace(cfg.Base) == "" {
			cfg.Base = "/PRE"
		}
		cfg.Groups = sortedAffils(cfg.Groups)
		return cfg
	}
	return affilsFileConfig{Base: "/PRE", Groups: sortedAffils(append([]Affil(nil), p.affils...))}
}

func (p *Plugin) saveAffilsFileConfig(cfg affilsFileConfig) error {
	if strings.TrimSpace(cfg.Base) == "" {
		cfg.Base = "/PRE"
	}
	target := p.affilsWritePath()
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return err
	}
	return os.WriteFile(target, data, 0644)
}

func (p *Plugin) affilsWritePath() string {
	for _, candidate := range affilsFileCandidates(p.file) {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	return affilsFileCandidates(p.file)[0]
}

func (p *Plugin) replies(evt *event.Event, lines ...string) []plugin.Output {
	target := strings.TrimSpace(evt.Data["channel"])
	noticeReply := false
	if strings.HasPrefix(p.replyTarget, "#") {
		target = p.replyTarget
	} else if p.replyTarget == "notice" || target == "" {
		target = evt.User
		noticeReply = true
	}
	out := make([]plugin.Output, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		out = append(out, plugin.Output{Type: "COMMAND", Target: target, Notice: noticeReply, Text: line})
	}
	return out
}

func (p *Plugin) reply(evt *event.Event, text string) []plugin.Output {
	return p.replies(evt, text)
}

func (p *Plugin) render(key string, vars map[string]string, fallback string) string {
	if p.theme != nil {
		if raw, ok := p.theme.Announces[key]; ok && raw != "" {
			return tmpl.Render(raw, vars)
		}
	}
	return fallback
}

func affilVars(affil Affil) map[string]string {
	return map[string]string{
		"group":  affil.Group,
		"predir": affil.Predir,
	}
}

func affilsConfig(raw interface{}) []Affil {
	switch v := raw.(type) {
	case []interface{}:
		out := make([]Affil, 0, len(v))
		for _, item := range v {
			switch m := item.(type) {
			case map[string]interface{}:
				group, _ := m["group"].(string)
				predir, _ := m["predir"].(string)
				group = strings.TrimSpace(group)
				predir = strings.TrimSpace(predir)
				if group != "" {
					out = append(out, Affil{Group: group, Predir: predir})
				}
			case map[interface{}]interface{}:
				group, _ := m["group"].(string)
				predir, _ := m["predir"].(string)
				group = strings.TrimSpace(group)
				predir = strings.TrimSpace(predir)
				if group != "" {
					out = append(out, Affil{Group: group, Predir: predir})
				}
			case string:
				if group := strings.TrimSpace(m); group != "" {
					out = append(out, Affil{Group: group})
				}
			}
		}
		return out
	case []string:
		out := make([]Affil, 0, len(v))
		for _, group := range v {
			if group = strings.TrimSpace(group); group != "" {
				out = append(out, Affil{Group: group})
			}
		}
		return out
	case string:
		out := []Affil{}
		for _, group := range strings.Split(v, ",") {
			if group = strings.TrimSpace(group); group != "" {
				out = append(out, Affil{Group: group})
			}
		}
		return out
	default:
		return nil
	}
}

func configValue(section, flat map[string]interface{}, sectionKey, flatKey string) interface{} {
	raw, _ := configValueOK(section, flat, sectionKey, flatKey)
	return raw
}

func configValueOK(section, flat map[string]interface{}, sectionKey, flatKey string) (interface{}, bool) {
	if raw, ok := section[sectionKey]; ok {
		return raw, true
	}
	raw, ok := flat[flatKey]
	return raw, ok
}

func stringConfig(section, flat map[string]interface{}, sectionKey, flatKey string) (string, bool) {
	raw, ok := configValueOK(section, flat, sectionKey, flatKey)
	if !ok {
		return "", false
	}
	s, ok := raw.(string)
	return s, ok
}

func boolConfig(raw interface{}) (bool, bool) {
	switch v := raw.(type) {
	case bool:
		return v, true
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true", "yes", "1", "on":
			return true, true
		case "false", "no", "0", "off":
			return false, true
		}
	}
	return false, false
}

func cleanAbs(p string) string {
	p = strings.TrimSpace(strings.ReplaceAll(p, "\\", "/"))
	if p == "" {
		return "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return path.Clean(p)
}

func validAffilGroup(group string) bool {
	group = strings.TrimSpace(group)
	if group == "" || strings.ContainsAny(group, `/\:*?"<>|`) {
		return false
	}
	for _, r := range group {
		if r <= 32 {
			return false
		}
	}
	return true
}

func loadAffilsFile(filePath string) (affilsFileConfig, error) {
	var cfg affilsFileConfig
	for _, candidate := range affilsFileCandidates(filePath) {
		data, err := os.ReadFile(candidate)
		if err != nil {
			continue
		}
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return cfg, err
		}
		return cfg, nil
	}
	return cfg, fmt.Errorf("affils file not found: %s", filePath)
}

func affilsFileCandidates(filePath string) []string {
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		filePath = "etc/affils.yml"
	}
	out := []string{filePath}
	clean := filepath.ToSlash(filePath)
	if strings.HasPrefix(clean, "etc/") {
		out = append(out, "../"+clean)
	}
	return out
}

func sortedAffils(in []Affil) []Affil {
	out := make([]Affil, 0, len(in))
	for _, affil := range in {
		affil.Group = strings.TrimSpace(affil.Group)
		affil.Predir = strings.TrimSpace(affil.Predir)
		if affil.Group != "" {
			out = append(out, affil)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i].Group) < strings.ToLower(out[j].Group)
	})
	return out
}
