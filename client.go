package wss

import (
	"context"
	"log"
	"stream"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// ClientOption 定义了客户端的可选配置。
type ClientOption func(*Client)

// WithClientIOTimeout 设置了读写的超时时间。
func WithClientIOTimeout(d time.Duration) ClientOption {
	return func(c *Client) { c.ioTimeout = d }
}

// WithClientOpened 设置了连接建立时的回调函数。
func WithClientOpened(fn func()) ClientOption {
	return func(c *Client) { c.opened = fn }
}

// WithClientClosed 设置了连接关闭时的回调函数。
func WithClientClosed(fn func()) ClientOption {
	return func(c *Client) { c.closed = fn }
}

// WithClientErred 设置了发生错误时的回调函数。
func WithClientErred(fn func(err error)) ClientOption {
	return func(c *Client) { c.erred = fn }
}

// Client 是一个 WebSocket 客户端。
type Client struct {
	conn      *websocket.Conn
	stream    *stream.Stream[[]byte]
	send      *stream.Stream[writeRequest]
	ioTimeout time.Duration
	onceClose sync.Once
	cancel    context.CancelFunc
	opened    func()
	closed    func()
	erred     func(err error)
}

// NewClient 创建一个新的客户端。
func NewClient(ctx context.Context, addr string, opts ...ClientOption) (*Client, error) {
	conn, _, err := websocket.Dial(ctx, addr, nil)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{
		conn:      conn,
		stream:    stream.New[[]byte](1024),
		send:      stream.New[writeRequest](1024),
		ioTimeout: 2 * time.Minute,
		cancel:    cancel,
	}

	for _, o := range opts {
		o(client)
	}

	go client.readLoop(ctx)
	go client.writeMessages(ctx)

	if client.opened != nil {
		client.opened()
	}

	return client, nil
}

// readLoop 持续从 WebSocket 连接中读取消息。
func (c *Client) readLoop(ctx context.Context) {
	defer func() {
		c.stream.Close()
		c.Close()
	}()

	for {
		readCtx, cancel := withTimeoutIfSet(ctx, c.ioTimeout)
		msgType, msg, err := c.conn.Read(readCtx)
		cancel()

		if err != nil {
			if isExpectedCloseError(err) {
				return
			}
			log.Printf("client read error: %v", err)
			if c.erred != nil {
				c.erred(err)
			}
			return
		}

		switch msgType {
		case websocket.MessageBinary, websocket.MessageText:
			_ = c.stream.TryPut(msg)
		}
	}
}

func (c *Client) writeMessages(ctx context.Context) {
	defer func() {
		c.send.Close()
		c.failQueuedWrites(stream.ErrStreamClosed)
		c.Close()
	}()

	for {
		req, err := c.send.Get(ctx)
		if err != nil {
			return
		}

		writeCtx, cancel := withTimeoutIfSet(req.ctx, c.ioTimeout)
		err = c.conn.Write(writeCtx, websocket.MessageBinary, req.data)
		cancel()

		req.resolve(err)

		if err != nil {
			if isExpectedCloseError(err) {
				return
			}
			log.Printf("client write error: %v", err)
			if c.erred != nil {
				c.erred(err)
			}
			return
		}
	}
}

func (c *Client) failQueuedWrites(err error) {
	for {
		req, getErr := c.send.Get(context.Background())
		if getErr != nil {
			return
		}
		req.resolve(err)
	}
}

// Get 从客户端流中获取一条消息（阻塞）。
func (c *Client) Get(ctx context.Context) ([]byte, error) {
	return c.stream.Get(ctx)
}

// Put 发送一条二进制消息到服务端。
func (c *Client) Put(ctx context.Context, data []byte) error {
	req := newSendRequest(ctx, data)
	if err := c.send.Put(ctx, req); err != nil {
		return err
	}
	return req.waitResult(ctx)
}

// Close 关闭客户端连接。
func (c *Client) Close() {
	c.onceClose.Do(func() {
		c.cancel()
		c.send.Close()
		_ = c.conn.Close(websocket.StatusNormalClosure, "client shutdown")
		if c.closed != nil {
			c.closed()
		}
	})
}
