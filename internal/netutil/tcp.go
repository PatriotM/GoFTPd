package netutil

import "net"

const DefaultDataSocketBufferSize = 4 * 1024 * 1024

// ForceDataSocketBuffers pins SO_SNDBUF/SO_RCVBUF to the requested size instead
// of letting the kernel autotune. Only enable this if net.core.{r,w}mem_max are
// raised high enough that the request isn't clamped; otherwise it hurts, see
// below. Default off.
var ForceDataSocketBuffers = false

// ConfigureDataSocket tunes a data-transfer socket for throughput.
//
// We deliberately do NOT call SetReadBuffer/SetWriteBuffer by default. Calling
// either one disables Linux TCP buffer autotuning, after which the kernel clamps
// the fixed buffer to net.core.{r,w}mem_max (often only ~200-400KB on an untuned
// box). A pinned ~400KB send buffer caps single-stream throughput to buffer/RTT
// (~75 MB/s at a few ms RTT). Leaving autotuning on lets the window grow up to
// tcp_{r,w}mem[2] (not clamped by net.core), which is what glftpd-style daemons
// get: higher single-stream speed with the bursty ramp. TCP_NODELAY is always
// safe to set and helps the small trailing writes.
func ConfigureDataSocket(conn net.Conn, bufferSize int) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcpConn.SetNoDelay(true)
	if ForceDataSocketBuffers {
		if bufferSize < DefaultDataSocketBufferSize {
			bufferSize = DefaultDataSocketBufferSize
		}
		_ = tcpConn.SetReadBuffer(bufferSize)
		_ = tcpConn.SetWriteBuffer(bufferSize)
	}
}
