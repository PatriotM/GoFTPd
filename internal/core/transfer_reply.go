package core

import (
	"fmt"
	"net"
)

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
