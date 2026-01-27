// The MIT License (MIT)
//
// # Copyright (c) 2016 xtaci
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package std

import (
	"net"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
)

// ZstdStream is a net.Conn wrapper that compresses data using zstd
type ZstdStream struct {
	conn net.Conn
	w    *zstd.Encoder
	r    *zstd.Decoder
}

func (c *ZstdStream) Read(p []byte) (n int, err error) {
	return c.r.Read(p)
}

func (c *ZstdStream) Write(p []byte) (n int, err error) {
	if _, err := c.w.Write(p); err != nil {
		return 0, errors.WithStack(err)
	}

	if err := c.w.Flush(); err != nil {
		return 0, errors.WithStack(err)
	}
	return len(p), nil
}

func (c *ZstdStream) Close() error {
	c.w.Close()
	return c.conn.Close()
}

func (c *ZstdStream) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *ZstdStream) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *ZstdStream) SetDeadline(t time.Time) error {
	return c.conn.SetDeadline(t)
}

func (c *ZstdStream) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *ZstdStream) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

// NewZstdStream creates a new stream that compresses data using zstd
func NewZstdStream(conn net.Conn, windowSize int) (*ZstdStream, error) {
	c := new(ZstdStream)
	c.conn = conn

	var err error
	c.w, err = zstd.NewWriter(conn, zstd.WithEncoderConcurrency(1), zstd.WithEncoderLevel(zstd.SpeedBestCompression), zstd.WithWindowSize(windowSize))
	if err != nil {
		return nil, errors.Wrap(err, "zstd.NewWriter")
	}

	c.r, err = zstd.NewReader(conn, zstd.WithDecoderMaxWindow(uint64(windowSize)))
	if err != nil {
		return nil, errors.Wrap(err, "zstd.NewReader")
	}

	return c, nil
}
