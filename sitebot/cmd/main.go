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
	b := bot.NewBot(cfg)
	if err := b.Start(); err != nil { log.Fatalf("start: %v", err) }
	log.Println("GoSitebot running...")
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c
	_ = b.Stop()
}
