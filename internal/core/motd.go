package core

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const motdPath = "etc/motd"

// %U expands to the username and %V to the running version.
var defaultMOTDLines = []string{
	`   ____       _____ _____ ____     _`,
	`  / ___| ___ |  ___|_   _|  _ \ __| |`,
	" | |  _ / _ \\| |_    | | | |_) / _` |",
	` | |_| | (_) |  _|   | | |  __/ (_| |`,
	`  \____|\___/|_|     |_| |_|   \__,_|`,
	``,
	` Welcome to GoFTPd v%V, %U!`,
}

func (s *Session) emitLoginMOTD() {
	data, err := os.ReadFile(motdPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return
		}
		content := strings.Join(defaultMOTDLines, "\n") + "\n"
		_ = os.MkdirAll(filepath.Dir(motdPath), 0755)
		_ = os.WriteFile(motdPath, []byte(content), 0644)
		data = []byte(content)
	}

	version := ""
	if s.Config != nil {
		version = s.Config.Version
	}
	repl := strings.NewReplacer("%U", s.User.Name, "%V", version)
	normalized := strings.ReplaceAll(string(data), "\r\n", "\n")
	for _, line := range strings.Split(strings.TrimRight(normalized, "\n"), "\n") {
		fmt.Fprintf(s.Conn, "230-%s\r\n", repl.Replace(strings.TrimRight(line, "\r")))
	}
}
