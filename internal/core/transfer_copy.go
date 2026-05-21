package core

import (
	"io"
	"sync"
)

const transferCopyBufferSize = 256 * 1024

var transferCopyBufferPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, transferCopyBufferSize)
		return &buf
	},
}

func copyTransferData(dst io.Writer, src io.Reader) (int64, error) {
	bufPtr := transferCopyBufferPool.Get().(*[]byte)
	defer transferCopyBufferPool.Put(bufPtr)
	return io.CopyBuffer(dst, src, *bufPtr)
}
