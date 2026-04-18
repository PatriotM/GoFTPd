package bot

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Debug      bool           `yaml:"debug"`
	EventFIFO  string         `yaml:"event_fifo"`
	IRC        IRCConfig      `yaml:"irc"`
	Encryption EncConfig      `yaml:"encryption"`
	Announce   AnnounceConfig `yaml:"announce"`
	Sections   []SectionRoute `yaml:"sections"`
	Plugins    PluginsConfig  `yaml:"plugins"`
}

type IRCConfig struct {
	Host          string   `yaml:"host"`
	Port          int      `yaml:"port"`
	SSL           bool     `yaml:"ssl"`
	Nick          string   `yaml:"nick"`
	User          string   `yaml:"user"`
	RealName      string   `yaml:"realname"`
	Password      string   `yaml:"password"`
	Channels      []string `yaml:"channels"`
	OperUser      string   `yaml:"operuser"`
	OperPassword  string   `yaml:"operpassword"`
	AutoOper      bool     `yaml:"auto_oper"`
	UserModes     string   `yaml:"user_modes"`
	AutoJoinDelay int      `yaml:"autojoin_delay_ms"`
}

type EncConfig struct {
	Enabled bool              `yaml:"enabled"`
	Keys    map[string]string `yaml:"keys"`
}

type AnnounceConfig struct {
	DefaultChannel string              `yaml:"default_channel"`
	TypeRoutes     map[string][]string `yaml:"type_routes"`
	ThemeFile      string              `yaml:"theme_file"`
}

type SectionRoute struct {
	Name     string   `yaml:"name"`
	Channels []string `yaml:"channels"`
	Paths    []string `yaml:"paths"`
}

type PluginsConfig struct {
	Enabled map[string]bool        `yaml:"enabled"`
	Config  map[string]interface{} `yaml:"config"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	if cfg.Announce.TypeRoutes == nil {
		cfg.Announce.TypeRoutes = map[string][]string{}
	}
	if cfg.Encryption.Keys == nil {
		cfg.Encryption.Keys = map[string]string{}
	}
	if cfg.Plugins.Enabled == nil {
		cfg.Plugins.Enabled = map[string]bool{}
	}
	if cfg.Plugins.Config == nil {
		cfg.Plugins.Config = map[string]interface{}{}
	}
	if cfg.Announce.ThemeFile == "" {
		cfg.Announce.ThemeFile = "./etc/templates/pzsng.theme"
	}
	return cfg, nil
}
