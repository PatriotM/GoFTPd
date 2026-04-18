package plugin

import (
	"sort"

	"goftpd/sitebot/internal/event"
)

type Handler interface {
	Name() string
	Initialize(config map[string]interface{}) error
	OnEvent(evt *event.Event) ([]Output, error)
	Close() error
}

type Output struct {
	Type string
	Text string
}

type Manager struct{ plugins map[string]Handler }

func NewManager() *Manager                  { return &Manager{plugins: map[string]Handler{}} }
func (m *Manager) Register(p Handler) error { m.plugins[p.Name()] = p; return nil }
func (m *Manager) List() []string {
	names := []string{}
	for n := range m.plugins {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
func (m *Manager) ProcessEvent(evt *event.Event) ([]Output, error) {
	var outs []Output
	for _, name := range m.List() {
		o, err := m.plugins[name].OnEvent(evt)
		if err == nil {
			outs = append(outs, o...)
		}
	}
	return outs, nil
}
func (m *Manager) Close() error {
	for _, name := range m.List() {
		_ = m.plugins[name].Close()
	}
	return nil
}
