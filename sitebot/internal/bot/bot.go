package bot

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"goftpd/sitebot/internal/event"
	"goftpd/sitebot/internal/irc"
	"goftpd/sitebot/internal/plugin"
)

type Bot struct {
	Config    *Config
	IRC       *irc.Bot
	Plugins   *plugin.Manager
	EventChan chan *event.Event
	Done      chan bool
	Mutex     sync.RWMutex
	Debug     bool
}

func NewBot(cfg *Config) *Bot {
	return &Bot{Config: cfg, Plugins: plugin.NewManager(), EventChan: make(chan *event.Event, 1024), Done: make(chan bool), Debug: cfg.Debug}
}

func (b *Bot) Start() error {
	if b.Debug {
		log.Println("[Bot] Starting GoSitebot")
	}
	b.IRC = irc.NewBot(b.Config.IRC.Host, b.Config.IRC.Port, b.Config.IRC.Nick, b.Config.IRC.User, b.Config.IRC.RealName)
	b.IRC.SSL = b.Config.IRC.SSL
	b.IRC.Password = b.Config.IRC.Password
	b.IRC.Debug = b.Debug
	if err := b.IRC.Connect(); err != nil {
		return fmt.Errorf("failed to connect to IRC: %w", err)
	}
	for ch, key := range b.Config.Encryption.Keys {
		_ = b.IRC.SetChannelKey(ch, key)
	}
	if err := b.initializePlugins(); err != nil {
		return err
	}
	go b.listenIRC()
	go b.readEvents()
	go b.processEvents()
	return nil
}

func (b *Bot) initializePlugins() error {
	if enabled, ok := b.Config.Plugins.Enabled["Announce"]; !ok || enabled {
		announce := plugin.NewAnnouncePlugin()
		cfg := map[string]interface{}{"debug": b.Debug, "theme_file": b.Config.Announce.ThemeFile}
		for k, v := range b.Config.Plugins.Config {
			cfg[k] = v
		}
		if err := announce.Initialize(cfg); err != nil {
			return err
		}
		if err := b.Plugins.Register(announce); err != nil {
			return err
		}
	}
	if enabled, ok := b.Config.Plugins.Enabled["TVMaze"]; !ok || enabled {
		tv := plugin.NewTVMazePlugin()
		cfg := map[string]interface{}{"debug": b.Debug, "theme_file": b.Config.Announce.ThemeFile}
		for k, v := range b.Config.Plugins.Config {
			cfg[k] = v
		}
		if err := tv.Initialize(cfg); err != nil {
			return err
		}
		if err := b.Plugins.Register(tv); err != nil {
			return err
		}
	}
	if b.Debug {
		log.Printf("[Bot] Loaded %d plugins: %v", len(b.Plugins.List()), b.Plugins.List())
	}
	return nil
}

func uniqueChannels(cfg *Config) []string {
	set := map[string]bool{}
	add := func(ch string) {
		ch = strings.TrimSpace(ch)
		if ch != "" {
			set[ch] = true
		}
	}
	for _, ch := range cfg.IRC.Channels {
		add(ch)
	}
	add(cfg.Announce.DefaultChannel)
	for _, arr := range cfg.Announce.TypeRoutes {
		for _, ch := range arr {
			add(ch)
		}
	}
	for _, s := range cfg.Sections {
		for _, ch := range s.Channels {
			add(ch)
		}
	}
	out := []string{}
	for ch := range set {
		out = append(out, ch)
	}
	return out
}

func (b *Bot) listenIRC() {
	var registeredOnce sync.Once
	handler := func(line string) {
		if strings.Contains(line, " 004 ") || strings.Contains(line, " 376 ") || strings.Contains(line, " 422 ") {
			registeredOnce.Do(func() {
				b.onRegistered()
			})
			return
		}

		if i := strings.Index(line, " INVITE "); i != -1 {
			parts := strings.Split(line, " ")
			if len(parts) >= 4 {
				channel := strings.TrimPrefix(parts[3], ":")
				if channel != "" {
					if b.Debug {
						log.Printf("[Bot] INVITE received, joining %s", channel)
					}
					_ = b.IRC.Join(channel)
				}
			}
		}
	}
	if err := b.IRC.Listen(handler); err != nil {
		log.Printf("[Bot] IRC listen error: %v", err)
	}
}

func (b *Bot) onRegistered() {
	operUser := strings.TrimSpace(b.Config.IRC.OperUser)
	if operUser == "" {
		operUser = b.Config.IRC.Nick
	}
	if b.Config.IRC.AutoOper && strings.TrimSpace(b.Config.IRC.OperPassword) != "" {
		if err := b.IRC.SendRaw(fmt.Sprintf("OPER %s %s", operUser, b.Config.IRC.OperPassword)); err != nil && b.Debug {
			log.Printf("[Bot] OPER failed: %v", err)
		}
		if d := b.Config.IRC.AutoJoinDelay; d > 0 {
			time.Sleep(time.Duration(d) * time.Millisecond)
		} else {
			time.Sleep(1500 * time.Millisecond)
		}
	}

	if modes := strings.TrimSpace(b.Config.IRC.UserModes); modes != "" {
		modeArg := modes
		if !strings.HasPrefix(modeArg, "+") && !strings.HasPrefix(modeArg, "-") {
			modeArg = "+" + modeArg
		}
		if err := b.IRC.SendRaw(fmt.Sprintf("MODE %s %s", b.Config.IRC.Nick, modeArg)); err != nil && b.Debug {
			log.Printf("[Bot] MODE failed: %v", err)
		}
	}

	for _, ch := range uniqueChannels(b.Config) {
		_ = b.IRC.Join(ch)
		if d := b.Config.IRC.AutoJoinDelay; d > 0 {
			time.Sleep(time.Duration(d) * time.Millisecond)
		}
	}
}

func (b *Bot) readEvents() {
	for {
		for {
			if _, err := os.Stat(b.Config.EventFIFO); err == nil {
				break
			}
			time.Sleep(time.Second)
		}
		f, err := os.Open(b.Config.EventFIFO)
		if err != nil {
			log.Printf("[Bot] Failed to open FIFO: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		s := bufio.NewScanner(f)
		for s.Scan() {
			evt, err := parseEvent(s.Text())
			if err != nil {
				continue
			}
			select {
			case b.EventChan <- evt:
			case <-b.Done:
				_ = f.Close()
				return
			}
		}
		_ = f.Close()
	}
}
func (b *Bot) processEvents() {
	for {
		select {
		case evt := <-b.EventChan:
			b.handleEvent(evt)
		case <-b.Done:
			return
		}
	}
}

func pathMatches(pattern, p string) bool {
	if pattern == "" || p == "" {
		return false
	}
	if ok, _ := filepath.Match(strings.TrimSpace(pattern), p); ok {
		return true
	}
	return strings.HasPrefix(strings.ToLower(p), strings.ToLower(strings.TrimRight(pattern, "*")))
}
func (b *Bot) routeChannels(evt *event.Event, outType string) []string {
	if chs := b.Config.Announce.TypeRoutes[outType]; len(chs) > 0 {
		return chs
	}
	for _, sec := range b.Config.Sections {
		if strings.EqualFold(sec.Name, evt.Section) {
			return sec.Channels
		}
		for _, pat := range sec.Paths {
			if pathMatches(pat, evt.Path) {
				return sec.Channels
			}
		}
	}
	if strings.TrimSpace(b.Config.Announce.DefaultChannel) != "" {
		return []string{b.Config.Announce.DefaultChannel}
	}
	return b.Config.IRC.Channels
}
func (b *Bot) handleEvent(evt *event.Event) {
	outs, err := b.Plugins.ProcessEvent(evt)
	if err != nil {
		return
	}
	for _, out := range outs {
		channels := b.routeChannels(evt, out.Type)
		for _, line := range strings.Split(out.Text, "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			for _, ch := range channels {
				_ = b.IRC.SendMessage(ch, line)
			}
		}
	}
}
func (b *Bot) Stop() error {
	close(b.Done)
	if b.IRC != nil {
		_ = b.IRC.Quit("Shutting down")
		_ = b.IRC.Close()
	}
	_ = b.Plugins.Close()
	return nil
}

func parseEvent(line string) (*event.Event, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil, fmt.Errorf("empty")
	}
	if strings.HasPrefix(line, "{") {
		var evt event.Event
		if err := json.Unmarshal([]byte(line), &evt); err != nil {
			return nil, err
		}
		if evt.Data == nil {
			evt.Data = map[string]string{}
		}
		return &evt, nil
	}
	parts := strings.Split(line, ":")
	if len(parts) < 4 {
		return nil, fmt.Errorf("invalid")
	}
	evt := event.NewEvent(event.EventType(strings.ToUpper(parts[0])), parts[1], parts[2], parts[3], "")
	if len(parts) > 4 {
		evt.Filename = parts[4]
	}
	if len(parts) > 5 {
		fmt.Sscanf(parts[5], "%d", &evt.Size)
	}
	if len(parts) > 6 {
		fmt.Sscanf(parts[6], "%f", &evt.Speed)
	}
	if len(parts) > 7 {
		evt.Path = parts[7]
	}
	return evt, nil
}
