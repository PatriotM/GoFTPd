package slave

import (
	"context"
	"crypto/tls"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"goftpd/internal/protocol"
)

const (
	TransferUnknown    = 'U'
	TransferReceiving  = 'R' // upload from client to slave
	TransferSending    = 'S' // download from slave to client
	transferPollTick   = 100 * time.Millisecond
	transferStatusTick = time.Second
)

// Transfer represents a data transfer on the slave side.
type Transfer struct {
	listener      net.Listener // non-nil for passive (LISTEN)
	conn          net.Conn     // non-nil for active (CONNECT) or after passive accept
	transferIndex int32
	slave         *Slave
	encrypted     bool
	sslClientMode bool
	activeAddress string
	minSpeed      int64
	maxSpeed      int64
	minSpeedGrace time.Duration
	mode          byte

	path        string
	direction   byte
	started     time.Time
	finished    time.Time
	transferred int64
	checksum    uint32
	abortReason string
	mu          sync.Mutex
}

func NewTransfer(listener net.Listener, conn net.Conn, idx int32, slave *Slave, encrypted bool, sslClientMode bool) *Transfer {
	return &Transfer{
		listener:      listener,
		conn:          conn,
		transferIndex: idx,
		slave:         slave,
		encrypted:     encrypted,
		sslClientMode: sslClientMode,
		direction:     TransferUnknown,
		mode:          'I',
	}
}

func (t *Transfer) SetActiveAddress(address string) {
	t.mu.Lock()
	t.activeAddress = strings.TrimSpace(address)
	t.mu.Unlock()
}

func (t *Transfer) SetSpeedLimits(minSpeed, maxSpeed int64, graceSeconds int64) {
	t.mu.Lock()
	t.minSpeed = minSpeed
	t.maxSpeed = maxSpeed
	if graceSeconds < 0 {
		graceSeconds = 0
	}
	t.minSpeedGrace = time.Duration(graceSeconds) * time.Second
	t.mu.Unlock()
}

func (t *Transfer) SetTransferMode(mode byte) {
	t.mu.Lock()
	switch mode {
	case 'A', 'I':
		t.mode = mode
	default:
		t.mode = 'I'
	}
	t.mu.Unlock()
}

func (t *Transfer) SetPath(path string) {
	t.mu.Lock()
	t.path = path
	t.mu.Unlock()
}

func (t *Transfer) Path() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.path
}

func (t *Transfer) SnapshotLiveStat() protocol.TransferLiveStat {
	t.mu.Lock()
	defer t.mu.Unlock()

	var startedUnixMs int64
	var speedBytes int64
	if !t.started.IsZero() {
		startedUnixMs = t.started.UnixMilli()
		elapsedMs := time.Since(t.started).Milliseconds()
		if elapsedMs > 0 && t.transferred > 0 {
			speedBytes = t.transferred * 1000 / elapsedMs
		}
	}

	return protocol.TransferLiveStat{
		TransferIndex: t.transferIndex,
		Direction:     t.direction,
		Path:          t.path,
		StartedUnixMs: startedUnixMs,
		Transferred:   t.transferred,
		SpeedBytes:    speedBytes,
	}
}

// ReceiveFile receives data from the FTP client and writes to disk.
// ().
func (t *Transfer) ReceiveFile(path string, position int64, expectedPeer string) protocol.TransferStatus {
	t.mu.Lock()
	t.direction = TransferUnknown
	t.started = time.Time{}
	t.mu.Unlock()

	defer func() {
		t.mu.Lock()
		t.finished = time.Now()
		t.mu.Unlock()
		t.slave.removeTransfer(t.transferIndex)
	}()

	if err := t.acceptPassiveConn(); err != nil {
		return t.errorStatus(err.Error())
	}

	if t.conn == nil {
		return t.errorStatus("no connection")
	}
	defer t.conn.Close()

	// Determine where to write
	fullPath, err := t.slave.getDirForUpload(path)
	if err != nil {
		return t.errorStatus(fmt.Sprintf("cannot create file: %v", err))
	}
	if position <= 0 {
		if _, statErr := os.Stat(fullPath); statErr == nil {
			return t.errorStatus(fmt.Sprintf("File %s exists", path))
		}
	}

	var file *os.File
	if position > 0 {
		file, err = os.OpenFile(fullPath, os.O_WRONLY, 0644)
		if err != nil {
			return t.errorStatus(fmt.Sprintf("resume open failed: %v", err))
		}
		if _, err := file.Seek(position, io.SeekStart); err != nil {
			file.Close()
			return t.errorStatus(fmt.Sprintf("resume seek failed: %v", err))
		}
	} else {
		file, err = os.Create(fullPath)
	}
	if err != nil {
		return t.errorStatus(fmt.Sprintf("create failed: %v", err))
	}
	defer file.Close()
	if isUnexpectedTransferPeer(expectedPeer, t.conn.RemoteAddr()) {
		cleanupFailedReceive(file, fullPath, position)
		return t.errorStatus("The IP that connected to the socket was not the one that was expected.")
	}
	t.mu.Lock()
	t.direction = TransferReceiving
	t.started = time.Now()
	t.mu.Unlock()

	// Transfer with CRC32
	h := crc32.NewIEEE()
	var out io.Writer = io.MultiWriter(file, h)
	buf := make([]byte, t.slave.getTransferBufferSize())
	lastStatus := time.Now()
	firstMinCheck := true
	lastMinCheck := time.Now()
	nextReadDeadline := time.Now().Add(transferPollTick)
	_ = t.conn.SetReadDeadline(nextReadDeadline)

	for {
		if t.abortReason != "" {
			cleanupFailedReceive(file, fullPath, position)
			return t.errorStatus("aborted: " + t.abortReason)
		}

		n, err := t.conn.Read(buf)
		if n > 0 {
			out.Write(buf[:n])
			t.mu.Lock()
			t.transferred += int64(n)
			t.mu.Unlock()
			t.applyMaxSpeed()
		}
		if time.Since(lastStatus) >= transferStatusTick {
			_ = t.slave.writeObject(&protocol.AsyncResponseTransferStatus{Status: t.currentStatus(false, "")})
			lastStatus = time.Now()
		}
		if err := t.checkMinSpeed(&lastMinCheck, &firstMinCheck); err != nil {
			cleanupFailedReceive(file, fullPath, position)
			return t.errorStatus(err.Error())
		}
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				nextReadDeadline = time.Now().Add(transferPollTick)
				_ = t.conn.SetReadDeadline(nextReadDeadline)
				continue
			}
			if err == io.EOF {
				break
			}
			cleanupFailedReceive(file, fullPath, position)
			return t.errorStatus(fmt.Sprintf("read error: %v", err))
		}
		if time.Until(nextReadDeadline) <= 0 {
			nextReadDeadline = time.Now().Add(transferPollTick)
			_ = t.conn.SetReadDeadline(nextReadDeadline)
		}
	}

	log.Printf("[Transfer] Received %s (%d bytes, CRC32=%08X, offset=%d)", path, t.transferred, h.Sum32(), position)
	t.mu.Lock()
	t.checksum = h.Sum32()
	t.mu.Unlock()
	_ = t.slave.writeObject(&protocol.AsyncResponseDiskStatus{Status: t.slave.getDiskStatus()})

	return protocol.TransferStatus{
		TransferIndex: t.transferIndex,
		Elapsed:       time.Since(t.started).Milliseconds(),
		Transferred:   t.transferred,
		Checksum:      h.Sum32(),
		Finished:      true,
	}
}

func cleanupFailedReceive(file *os.File, fullPath string, position int64) {
	if position <= 0 {
		_ = os.Remove(fullPath)
		return
	}
	if file != nil {
		_ = file.Sync()
		_ = file.Truncate(position)
		_, _ = file.Seek(position, io.SeekStart)
	}
}

// SendFile sends a file from disk to the FTP client.
// ().
func (t *Transfer) SendFile(path string, position int64, expectedPeer string) protocol.TransferStatus {
	t.mu.Lock()
	t.direction = TransferUnknown
	t.started = time.Time{}
	t.mu.Unlock()

	defer func() {
		t.mu.Lock()
		t.finished = time.Now()
		t.mu.Unlock()
		t.slave.removeTransfer(t.transferIndex)
	}()

	if err := t.acceptPassiveConn(); err != nil {
		return t.errorStatus(err.Error())
	}

	if t.conn == nil {
		return t.errorStatus("no connection")
	}
	defer t.conn.Close()
	if isUnexpectedTransferPeer(expectedPeer, t.conn.RemoteAddr()) {
		return t.errorStatus("The IP that connected to the socket was not the one that was expected.")
	}
	t.mu.Lock()
	t.direction = TransferSending
	t.started = time.Now()
	t.mu.Unlock()

	// Find the file across roots
	fullPath, err := t.slave.getFileFromRoots(path)
	if err != nil {
		return t.errorStatus(fmt.Sprintf("file not found: %v", err))
	}

	file, err := os.Open(fullPath)
	if err != nil {
		return t.errorStatus(fmt.Sprintf("open failed: %v", err))
	}
	defer file.Close()
	if position > 0 {
		if _, err := file.Seek(position, io.SeekStart); err != nil {
			return t.errorStatus(fmt.Sprintf("resume seek failed: %v", err))
		}
	}

	// Transfer with CRC32
	h := crc32.NewIEEE()
	r := io.TeeReader(file, h)
	buf := make([]byte, t.slave.getTransferBufferSize())
	lastStatus := time.Now()
	firstMinCheck := true
	lastMinCheck := time.Now()
	associatedUpload := t.findAssociatedUpload(path)
	var writer io.Writer = t.conn

	for {
		if t.abortReason != "" {
			return t.errorStatus("aborted: " + t.abortReason)
		}

		n, err := r.Read(buf)
		if n > 0 {
			_, werr := writer.Write(buf[:n])
			if werr != nil {
				return t.errorStatus(fmt.Sprintf("write error: %v", werr))
			}
			t.mu.Lock()
			t.transferred += int64(n)
			t.mu.Unlock()
			t.applyMaxSpeed()
		}
		if time.Since(lastStatus) >= transferStatusTick {
			_ = t.slave.writeObject(&protocol.AsyncResponseTransferStatus{Status: t.currentStatus(false, "")})
			lastStatus = time.Now()
		}
		if err := t.checkMinSpeed(&lastMinCheck, &firstMinCheck); err != nil {
			return t.errorStatus(err.Error())
		}
		if err != nil {
			if err == io.EOF {
				if associatedUpload == nil || associatedUpload.isFinished() {
					break
				}
				time.Sleep(transferPollTick)
				continue
			}
			return t.errorStatus(fmt.Sprintf("read error: %v", err))
		}
	}

	log.Printf("[Transfer] Sent %s (%d bytes, CRC32=%08X, offset=%d)", path, t.transferred, h.Sum32(), position)
	t.mu.Lock()
	t.checksum = h.Sum32()
	t.mu.Unlock()

	return protocol.TransferStatus{
		TransferIndex: t.transferIndex,
		Elapsed:       time.Since(t.started).Milliseconds(),
		Transferred:   t.transferred,
		Checksum:      h.Sum32(),
		Finished:      true,
	}
}

func (t *Transfer) acceptPassiveConn() error {
	if t.conn != nil {
		return nil
	}
	if t.listener == nil {
		return t.connectActive()
	}
	if deadlineListener, ok := t.listener.(interface{ SetDeadline(time.Time) error }); ok {
		deadlineListener.SetDeadline(time.Now().Add(10 * time.Second))
	}
	conn, err := t.listener.Accept()
	t.listener.Close()
	if err != nil {
		return fmt.Errorf("accept failed: %v", err)
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
	}
	if !t.encrypted {
		t.conn = conn
		return nil
	}

	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetDeadline(time.Time{})

	if t.sslClientMode {
		tlsConn := tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
		if err := tlsConn.Handshake(); err != nil {
			conn.Close()
			return fmt.Errorf("TLS client handshake failed: %v", err)
		}
		t.conn = tlsConn
		return nil
	}

	cert, err := tls.LoadX509KeyPair(t.slave.tlsCert, t.slave.tlsKey)
	if err != nil {
		conn.Close()
		return fmt.Errorf("load TLS cert: %v", err)
	}
	tlsConn := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{cert}})
	if err := tlsConn.Handshake(); err != nil {
		conn.Close()
		return fmt.Errorf("TLS server handshake failed: %v", err)
	}
	t.conn = tlsConn
	return nil
}

func (t *Transfer) connectActive() error {
	t.mu.Lock()
	address := t.activeAddress
	t.mu.Unlock()
	if address == "" {
		return fmt.Errorf("no connection")
	}

	dialer := &net.Dialer{Timeout: 10 * time.Second}
	if bindIP := strings.TrimSpace(t.slave.bindIP); bindIP != "" {
		if ip := net.ParseIP(bindIP); ip != nil {
			dialer.LocalAddr = &net.TCPAddr{IP: ip}
		}
	}
	conn, err := dialer.Dial("tcp", address)
	if err != nil {
		return fmt.Errorf("connect failed: %v", err)
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
	}

	if !t.encrypted {
		t.conn = conn
		return nil
	}

	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetDeadline(time.Time{})

	if t.sslClientMode {
		tlsConn := tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
		if err := tlsConn.Handshake(); err != nil {
			conn.Close()
			return fmt.Errorf("TLS client handshake failed: %v", err)
		}
		t.conn = tlsConn
		return nil
	}

	cert, err := tls.LoadX509KeyPair(t.slave.tlsCert, t.slave.tlsKey)
	if err != nil {
		conn.Close()
		return fmt.Errorf("load TLS cert: %v", err)
	}
	tlsConn := tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{cert}})
	if err := tlsConn.Handshake(); err != nil {
		conn.Close()
		return fmt.Errorf("TLS server handshake failed: %v", err)
	}
	t.conn = tlsConn
	return nil
}

func (t *Transfer) Abort(reason string) {
	t.mu.Lock()
	t.abortReason = reason
	t.mu.Unlock()
	if t.conn != nil {
		t.conn.Close()
	}
	if t.listener != nil {
		t.listener.Close()
	}
}

func (t *Transfer) errorStatus(msg string) protocol.TransferStatus {
	return t.currentStatus(true, msg)
}

func (t *Transfer) isFinished() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return !t.finished.IsZero() || t.abortReason != ""
}

func (t *Transfer) findAssociatedUpload(path string) *Transfer {
	path = filepath.ToSlash(strings.TrimSpace(path))
	if path == "" {
		return nil
	}
	var upload *Transfer
	t.slave.transfers.Range(func(_, value interface{}) bool {
		other, ok := value.(*Transfer)
		if !ok || other == nil || other == t {
			return true
		}
		other.mu.Lock()
		otherPath := filepath.ToSlash(strings.TrimSpace(other.path))
		otherDir := other.direction
		otherFinished := !other.finished.IsZero() || other.abortReason != ""
		other.mu.Unlock()
		if otherDir == TransferReceiving && !otherFinished && strings.EqualFold(otherPath, path) {
			upload = other
			return false
		}
		return true
	})
	return upload
}

func (t *Transfer) currentStatus(finished bool, errMsg string) protocol.TransferStatus {
	elapsed := int64(0)
	if !t.started.IsZero() {
		elapsed = time.Since(t.started).Milliseconds()
	}
	return protocol.TransferStatus{
		TransferIndex: t.transferIndex,
		Elapsed:       elapsed,
		Transferred:   t.transferred,
		Checksum:      t.checksum,
		Finished:      finished,
		Error:         errMsg,
	}
}

func (t *Transfer) applyMaxSpeed() {
	t.mu.Lock()
	maxSpeed := t.maxSpeed
	started := t.started
	transferred := t.transferred
	t.mu.Unlock()
	if maxSpeed <= 0 || started.IsZero() || transferred <= 0 {
		return
	}
	elapsed := time.Since(started)
	expected := time.Duration(float64(transferred) / float64(maxSpeed) * float64(time.Second))
	if expected > elapsed {
		time.Sleep(expected - elapsed)
	}
}

func (t *Transfer) checkMinSpeed(lastCheck *time.Time, first *bool) error {
	t.mu.Lock()
	minSpeed := t.minSpeed
	started := t.started
	transferred := t.transferred
	grace := t.minSpeedGrace
	t.mu.Unlock()
	if minSpeed <= 0 || started.IsZero() || transferred <= 0 {
		return nil
	}
	if grace > 0 && time.Since(started) < grace {
		return nil
	}
	delay := 200 * time.Millisecond
	if *first {
		delay = 500 * time.Millisecond
	}
	if time.Since(*lastCheck) < delay {
		return nil
	}
	elapsedMs := time.Since(started).Milliseconds()
	if elapsedMs <= 0 {
		*lastCheck = time.Now()
		*first = false
		return nil
	}
	currentSpeed := transferred * 1000 / elapsedMs
	*lastCheck = time.Now()
	*first = false
	if currentSpeed < minSpeed {
		return fmt.Errorf("transfer was aborted - '%d' is < '%d'", currentSpeed, minSpeed)
	}
	return nil
}

func isUnexpectedTransferPeer(expected string, remote net.Addr) bool {
	expected = strings.TrimSpace(expected)
	if expected == "" || strings.EqualFold(expected, "master") {
		return false
	}
	remoteHost := ""
	if remote != nil {
		if parsedHost, _, err := net.SplitHostPort(remote.String()); err == nil {
			remoteHost = parsedHost
		} else {
			remoteHost = remote.String()
		}
	}
	remoteHost = strings.TrimSpace(remoteHost)
	remoteIP := net.ParseIP(remoteHost)
	if remoteIP == nil && remoteHost == "" {
		return false
	}
	hostMask := expected
	if at := strings.LastIndex(hostMask, "@"); at >= 0 {
		hostMask = hostMask[at+1:]
	}
	if parsedHost, _, err := net.SplitHostPort(hostMask); err == nil {
		hostMask = parsedHost
	}
	hostMask = strings.TrimSpace(hostMask)
	if hostMask == "" {
		hostMask = "*"
	}
	return !matchesTransferHostMask(hostMask, remoteHost, remoteIP)
}

func matchesTransferHostMask(hostMask string, remoteHost string, remoteIP net.IP) bool {
	if hostMask == "*" {
		return true
	}
	candidates := make([]string, 0, 3)
	if remoteIP != nil {
		candidates = append(candidates, remoteIP.String())
	}
	if remoteHost != "" {
		candidates = append(candidates, remoteHost)
	}
	if shouldResolveTransferHostname(hostMask, remoteHost, remoteIP) {
		candidates = append(candidates, resolveTransferHostnames(remoteIP)...)
	}
	for _, candidate := range candidates {
		candidate = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(candidate)), ".")
		if candidate == "" {
			continue
		}
		if matched, err := path.Match(strings.ToLower(hostMask), candidate); err == nil && matched {
			return true
		}
	}
	return false
}

func shouldResolveTransferHostname(hostMask string, remoteHost string, remoteIP net.IP) bool {
	if remoteIP == nil {
		return false
	}
	for _, r := range hostMask {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '-' {
			return true
		}
	}
	return false
}

func resolveTransferHostnames(remoteIP net.IP) []string {
	if remoteIP == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	names, err := net.DefaultResolver.LookupAddr(ctx, remoteIP.String())
	if err != nil {
		return nil
	}
	results := make([]string, 0, len(names))
	for _, name := range names {
		name = strings.TrimSuffix(strings.TrimSpace(name), ".")
		if name != "" {
			results = append(results, name)
		}
	}
	return results
}
