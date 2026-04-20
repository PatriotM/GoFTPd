package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"goftpd/sitebot/internal/bot"
)

func main() {
	cfgPath := flag.String("config", "./etc/config.yml", "path to config")
	flag.Parse()
	cfg, err := bot.LoadConfig(*cfgPath)
	if err != nil { log.Fatalf("config: %v", err) }

	// File logging — only active when log_file is set. Tee's to stderr+file
	// with daily rotation (kept log_keep_days days, default 1).
	if cfg.LogFile != "" {
		if err := bot.InstallFileLogger(cfg.LogFile, cfg.LogKeepDays); err != nil {
			log.Printf("[LOG] file logger init failed: %v (continuing with stderr only)", err)
		}
	}

	b := bot.NewBot(cfg)
	if err := b.Start(); err != nil { log.Fatalf("start: %v", err) }
	log.Println("GoSitebot running...")
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	for s := range c {
		if s == syscall.SIGHUP {
			if path, err := cfg.Rehash(); err != nil {
				log.Printf("[REHASH] failed: %v", err)
			} else {
				log.Printf("[REHASH] reloaded %s", path)
			}
			continue
		}
		break
	}
	_ = b.Stop()
}
