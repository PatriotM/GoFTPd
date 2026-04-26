package bot

import (
	"testing"

	"goftpd/sitebot/internal/irc"
)

func TestResolveSectionTreatsForeignChildAsSection(t *testing.T) {
	b := &Bot{
		Config: &Config{
			Sections: []SectionRoute{
				{Name: "TV-NL", Paths: []string{"/FOREIGN/TV-NL/*"}},
			},
		},
	}

	if got := b.resolveSection("/FOREIGN/TV-NL", "FOREIGN"); got != "TV-NL" {
		t.Fatalf("resolveSection(%q) = %q, want %q", "/FOREIGN/TV-NL", got, "TV-NL")
	}

	if got := b.resolveSection("/FOREIGN/TV-NL/Some.Release-GRP", "FOREIGN"); got != "TV-NL" {
		t.Fatalf("resolveSection(%q) = %q, want %q", "/FOREIGN/TV-NL/Some.Release-GRP", got, "TV-NL")
	}
}

func TestPathMatchesIncludesSectionDirectoryForWildcardChildPattern(t *testing.T) {
	if !pathMatches("/FOREIGN/TV-NL/*", "/FOREIGN/TV-NL") {
		t.Fatalf("expected wildcard child pattern to match the section directory itself")
	}
}

func TestCommandEventFromPrivmsgDecryptsNegotiatedPMKey(t *testing.T) {
	cfg := &Config{
		IRC: IRCConfig{Nick: "GoSitebot"},
	}
	b := &Bot{
		Config: cfg,
		IRC:    irc.NewBot("irc.example.net", 6697, "GoSitebot", "sitebot", "GoSitebot"),
	}
	if err := b.IRC.SetPrivateUserKey("Alice", "cbc:SuperSecretPMKey123"); err != nil {
		t.Fatalf("SetPrivateUserKey: %v", err)
	}
	enc := b.IRC.PMKeys["alice"]
	line := ":Alice!user@example PRIVMSG GoSitebot :+OK *" + enc.Encrypt("!help")
	evt := b.commandEventFromPrivmsg(line)
	if evt == nil {
		t.Fatalf("expected decrypted command event")
	}
	if got := evt.Data["command"]; got != "help" {
		t.Fatalf("command = %q, want %q", got, "help")
	}
	if got := evt.User; got != "Alice" {
		t.Fatalf("user = %q, want %q", got, "Alice")
	}
}
