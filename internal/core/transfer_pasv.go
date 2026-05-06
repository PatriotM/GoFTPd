package core

import (
	"fmt"
	"net"
	"strings"
)

func ftpPassiveIPv4(addr string) (string, error) {
	ip := net.ParseIP(strings.TrimSpace(addr)).To4()
	if ip == nil {
		return "", fmt.Errorf("invalid passive IPv4 address")
	}
	return strings.ReplaceAll(ip.String(), ".", ","), nil
}
