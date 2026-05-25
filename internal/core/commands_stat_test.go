package core

import (
	"net"
	"strings"
	"testing"
)

func TestSTATWithoutLoginDoesNotPanic(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	done := make(chan string, 1)
	go func() {
		var response strings.Builder
		buf := make([]byte, 256)
		for {
			n, err := client.Read(buf)
			if n > 0 {
				response.Write(buf[:n])
				if strings.Contains(response.String(), "211 End of status.") {
					done <- response.String()
					return
				}
			}
			if err != nil {
				done <- response.String()
				return
			}
		}
	}()

	s := &Session{
		Conn:         server,
		Config:       &Config{SiteNameShort: "HV"},
		TransferType: "I",
	}

	if quit := s.processCommand("STAT", nil, nil); quit {
		t.Fatal("STAT should not terminate the session")
	}

	response := <-done
	if !strings.Contains(response, "211- HV server status:") {
		t.Fatalf("expected server status header, got %q", response)
	}
	if !strings.Contains(response, " Not logged in") {
		t.Fatalf("expected unauthenticated status, got %q", response)
	}
	if !strings.Contains(response, " TYPE: BINARY") {
		t.Fatalf("expected binary transfer type, got %q", response)
	}
}
