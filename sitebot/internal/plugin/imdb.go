package plugin

import "goftpd/sitebot/internal/event"

type IMDBPlugin struct{}
func NewIMDBPlugin() *IMDBPlugin { return &IMDBPlugin{} }
func (p *IMDBPlugin) Name() string { return "IMDB" }
func (p *IMDBPlugin) Initialize(config map[string]interface{}) error { return nil }
func (p *IMDBPlugin) OnEvent(evt *event.Event) ([]Output, error) { return nil, nil }
func (p *IMDBPlugin) Close() error { return nil }
