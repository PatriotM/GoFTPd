package core

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
)

var slowTransferErrorRE = regexp.MustCompile(`transfer was aborted - '([0-9]+)' is < '([0-9]+)'`)

func writeTransferFailure(conn net.Conn, operation string, err error) {
	if conn == nil {
		return
	}
	if err == nil {
		fmt.Fprintf(conn, "426 %s failed.\r\n", operation)
		return
	}
	fmt.Fprintf(conn, "426 %s failed: %v\r\n", operation, err)
}

func describeTransferFailure(err error) string {
	if err == nil {
		return "unknown transfer failure"
	}

	raw := err.Error()
	lower := strings.ToLower(raw)

	switch {
	case strings.Contains(lower, "transfer was aborted -"):
		return "transfer was aborted because the live transfer speed stayed below the configured minimum"
	case strings.Contains(lower, "tls server handshake failed") && strings.Contains(lower, "i/o timeout"):
		return "remote peer accepted the data connection but did not finish the TLS handshake in time"
	case strings.Contains(lower, "tls client handshake failed") && strings.Contains(lower, "i/o timeout"):
		return "remote peer did not complete the client-side TLS handshake in time"
	case strings.Contains(lower, "tls server handshake failed") && strings.Contains(lower, "connection reset by peer"):
		return "remote peer reset the connection during the TLS handshake"
	case strings.Contains(lower, "tls client handshake failed") && strings.Contains(lower, "connection reset by peer"):
		return "remote peer reset the connection during the client-side TLS handshake"
	case strings.Contains(lower, "connect failed") && strings.Contains(lower, "connection refused"):
		return "remote peer advertised a data port but nothing was listening on it"
	case strings.Contains(lower, "connect failed") && strings.Contains(lower, "i/o timeout"):
		return "remote peer advertised a data port but never accepted the connection"
	case strings.Contains(lower, "read error") && strings.Contains(lower, "connection reset by peer"):
		return "remote peer reset the data connection during transfer"
	case strings.Contains(lower, "write error") && strings.Contains(lower, "connection reset by peer"):
		return "remote peer closed the data connection while we were sending data"
	case strings.Contains(raw, "The IP that connected to the socket was not the one that was expected."):
		return "a different host than the announced data peer connected to the prepared socket"
	case strings.Contains(lower, "unexpected response from slave"):
		return "slave returned an unexpected async response"
	case strings.Contains(lower, "file not found on any available slave"):
		return "file was requested before it was available on any online slave"
	case strings.Contains(lower, "receive ack:") && strings.Contains(lower, "file not found:"):
		return "slave acknowledged setup but reported the source file missing before transfer start"
	case strings.Contains(lower, "send ack:") && strings.Contains(lower, "file not found:"):
		return "slave acknowledged setup but reported the download source missing before transfer start"
	case strings.Contains(lower, "file not found: file not found:"):
		return "slave reported the requested source file missing during transfer setup"
	case strings.Contains(lower, "file not found:"):
		return "requested source file was not available on the selected slave"
	default:
		return "transfer failed for an unclassified reason"
	}
}

func formatTransferFailureLog(err error) string {
	if err == nil {
		return "unknown transfer failure"
	}
	return fmt.Sprintf("%s (raw: %v)", describeTransferFailure(err), err)
}

func maybeHandleSlowTransfer(s *Session, direction, transferPath, slaveName string, transferIndex int32, err error) {
	if s == nil || s.Config == nil || s.Config.PluginManager == nil || err == nil {
		return
	}
	actualSpeedBytes, minSpeedBytes, ok := parseSlowTransferError(err)
	if !ok {
		return
	}
	username := ""
	primaryGroup := ""
	if s.User != nil {
		username = s.User.Name
		primaryGroup = s.User.PrimaryGroup
	}
	s.Config.PluginManager.HandleSlowTransfer(username, primaryGroup, transferPath, direction, slaveName, transferIndex, actualSpeedBytes, minSpeedBytes)
}

func parseSlowTransferError(err error) (int64, int64, bool) {
	if err == nil {
		return 0, 0, false
	}
	match := slowTransferErrorRE.FindStringSubmatch(err.Error())
	if len(match) != 3 {
		return 0, 0, false
	}
	actualSpeedBytes, err1 := strconv.ParseInt(match[1], 10, 64)
	minSpeedBytes, err2 := strconv.ParseInt(match[2], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return actualSpeedBytes, minSpeedBytes, true
}
