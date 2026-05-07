package core

import (
	"strings"
)

func transferTypeReplyName(transferType string) string {
	if strings.EqualFold(strings.TrimSpace(transferType), "A") {
		return "ASCII"
	}
	return "binary"
}
