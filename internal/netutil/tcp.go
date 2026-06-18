package netutil

import "net"

const DefaultDataSocketBufferSize = 4 * 1024 * 1024

func ConfigureDataSocket(conn net.Conn, bufferSize int) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	if bufferSize < DefaultDataSocketBufferSize {
		bufferSize = DefaultDataSocketBufferSize
	}
	_ = tcpConn.SetNoDelay(true)
	_ = tcpConn.SetReadBuffer(bufferSize)
	_ = tcpConn.SetWriteBuffer(bufferSize)
}
