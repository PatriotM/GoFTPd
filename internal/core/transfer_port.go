package core

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

func parsePortTarget(arg string) (net.IP, int, error) {
	parts := strings.Split(strings.TrimSpace(arg), ",")
	if len(parts) != 6 {
		return nil, 0, fmt.Errorf("syntax")
	}
	ipParts := make([]string, 4)
	for i := 0; i < 4; i++ {
		n, err := strconv.Atoi(strings.TrimSpace(parts[i]))
		if err != nil || n < 0 || n > 255 {
			return nil, 0, fmt.Errorf("syntax")
		}
		ipParts[i] = strconv.Itoa(n)
	}
	p1, err := strconv.Atoi(strings.TrimSpace(parts[4]))
	if err != nil || p1 < 0 || p1 > 255 {
		return nil, 0, fmt.Errorf("syntax")
	}
	p2, err := strconv.Atoi(strings.TrimSpace(parts[5]))
	if err != nil || p2 < 0 || p2 > 255 {
		return nil, 0, fmt.Errorf("syntax")
	}
	ip := net.ParseIP(strings.Join(ipParts, ".")).To4()
	if ip == nil {
		return nil, 0, fmt.Errorf("syntax")
	}
	return ip, p1*256 + p2, nil
}

func shouldRejectPortTarget(controlIP, dataIP net.IP) bool {
	if controlIP == nil || dataIP == nil {
		return false
	}
	control4 := controlIP.To4()
	data4 := dataIP.To4()
	if control4 == nil || data4 == nil {
		return false
	}
	return data4.IsPrivate() && !control4.IsPrivate()
}

func portTargetWarnings(controlIP, dataIP net.IP) []string {
	if controlIP == nil || dataIP == nil {
		return nil
	}
	var warnings []string
	if dataIP.IsLoopback() {
		warnings = append(warnings, "Ok, but distributed transfers won't work with local addresses")
	}
	if !dataIP.Equal(controlIP) {
		warnings = append(warnings, fmt.Sprintf("FXP allowed. If you're not FXPing then set your IP to %s", controlIP.String()))
	}
	return warnings
}
