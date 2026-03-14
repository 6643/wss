package wss

import (
	"context"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"stream"

	"github.com/coder/websocket"
)

// ServerOption 定义了服务端的可选配置。
type ServerOption func(*Server)

// WithServerIOTimeout 设置了读写的超时时间。
func WithServerIOTimeout(d time.Duration) ServerOption {
	return func(s *Server) { s.ioTimeout = d }
}

// WithServerBroadcastConcurrency 设置了广播的并发数。
func WithServerBroadcastConcurrency(n int) ServerOption {
	return func(s *Server) { s.broadcastConcurrency = n }
}

// WithServerCheckOrigin 设置了 Origin 的检查函数。
func WithServerCheckOrigin(fn func(r *http.Request) bool) ServerOption {
	return func(s *Server) { s.checkOrigin = fn }
}

// WithServerOpened 设置了新连接建立时的回调函数。
func WithServerOpened(fn func(conn *serverConn)) ServerOption {
	return func(s *Server) { s.opened = fn }
}

// WithServerClosed 设置了连接关闭时的回调函数。
func WithServerClosed(fn func(conn *serverConn)) ServerOption {
	return func(s *Server) { s.closed = fn }
}

// WithServerConnIdleTimeout 设置了连接的空闲超时时间。
func WithServerConnIdleTimeout(d time.Duration) ServerOption {
	return func(s *Server) { s.connIdleTimeout = d }
}

// WithServerErred 设置了发生错误时的回调函数。
func WithServerErred(fn func(conn *serverConn, err error)) ServerOption {
	return func(s *Server) { s.erred = fn }
}

// Server 是一个管理连接和消息的 WebSocket 服务端。
type Server struct {
	mu       sync.RWMutex
	conns    map[string]*serverConn
	stream   *stream.Stream[serverMessage]
	done     chan struct{}
	isClosed atomic.Bool

	ioTimeout            time.Duration
	connIdleTimeout      time.Duration
	broadcastConcurrency int

	checkOrigin func(*http.Request) bool
	opened      func(conn *serverConn)
	closed      func(conn *serverConn)
	erred       func(conn *serverConn, err error)

	wg        sync.WaitGroup
	closeOnce sync.Once
}

// serverMessage 代表一条从客户端到服务端的消息。
type serverMessage struct {
	conn *serverConn
	data []byte
}

// serverConn 代表一个客户端连接。
type serverConn struct {
	ws                   *websocket.Conn
	addr                 string
	header               http.Header
	ctx                  context.Context
	cancel               context.CancelFunc
	send                 *stream.Stream[writeRequest]
	onceClose            sync.Once
	lastActivityUnixNano atomic.Int64
}

// newServerConnection 创建一个新的服务端连接。
func newServerConnection(ws *websocket.Conn, r *http.Request) *serverConn {
	ctx, cancel := context.WithCancel(context.Background())
	sc := &serverConn{
		ws:     ws,
		addr:   r.RemoteAddr,
		header: r.Header,
		ctx:    ctx,
		cancel: cancel,
		send:   stream.New[writeRequest](1024),
	}
	sc.markActive()
	return sc
}

// Addr 返回连接的唯一标识。
func (sc *serverConn) Addr() string {
	return sc.addr
}

// Header 返回连接升级请求中的 HTTP 头。
func (sc *serverConn) Header() http.Header {
	return sc.header
}

func (sc *serverConn) markActive() {
	sc.lastActivityUnixNano.Store(time.Now().UnixNano())
}

func (sc *serverConn) lastActiveAt() time.Time {
	unixNano := sc.lastActivityUnixNano.Load()
	if unixNano == 0 {
		return time.Time{}
	}
	return time.Unix(0, unixNano)
}

// NewServer 创建一个新的服务端。
func NewServer(opts ...ServerOption) *Server {
	s := &Server{
		conns:                make(map[string]*serverConn),
		stream:               stream.New[serverMessage](4096),
		done:                 make(chan struct{}),
		ioTimeout:            2 * time.Minute,
		connIdleTimeout:      3 * time.Minute,
		broadcastConcurrency: 32,
	}
	for _, o := range opts {
		o(s)
	}
	if s.broadcastConcurrency < 1 {
		s.broadcastConcurrency = 1
	}
	go s.closeIdleConnections()

	return s
}

// closeIdleConnections 定期移除长时间空闲的连接。
func (s *Server) closeIdleConnections() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		if !s.tickIdle(ticker) {
			return
		}
	}
}

func (s *Server) tickIdle(ticker *time.Ticker) bool {
	select {
	case <-ticker.C:
		s.cleanupIdle()
		return true
	case <-s.done:
		return false
	}
}

func (s *Server) cleanupIdle() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, conn := range s.conns {
		s.checkAndCloseIdle(id, conn)
	}
}

func (s *Server) checkAndCloseIdle(id string, conn *serverConn) {
	if time.Since(conn.lastActiveAt()) <= s.connIdleTimeout {
		return
	}
	conn.closeWithStatus(websocket.StatusNormalClosure, "timeout")
	delete(s.conns, id)
}

// ServeHTTP 将一个 HTTP 连接升级为 WebSocket 并进行管理。
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.isClosed.Load() {
		http.Error(w, "server is closed", http.StatusServiceUnavailable)
		return
	}
	if s.checkOrigin != nil && !s.checkOrigin(r) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}
	s.acceptAndHandle(w, r)
}

func (s *Server) acceptAndHandle(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		log.Printf("upgrade failed: %v", err)
		return
	}
	s.handleAcceptedConn(c, r)
}

func (s *Server) handleAcceptedConn(c *websocket.Conn, r *http.Request) {
	conn := newServerConnection(c, r)

	s.mu.Lock()
	if s.isClosed.Load() {
		s.mu.Unlock()
		conn.closeWithStatus(websocket.StatusNormalClosure, "server is closed")
		return
	}
	s.conns[conn.addr] = conn
	s.mu.Unlock()

	if s.opened != nil {
		s.opened(conn)
	}

	s.wg.Add(1)
	go s.handleConnection(conn)
}

// handleConnection 处理单个连接。
func (s *Server) handleConnection(sc *serverConn) {
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		s.writeMessages(sc)
	}()

	defer func() {
		sc.closeWithStatus(websocket.StatusNormalClosure, "handler exit")
		<-writeDone
		s.mu.Lock()
		delete(s.conns, sc.addr)
		s.mu.Unlock()
		if s.closed != nil {
			s.closed(sc)
		}
		s.wg.Done()
	}()

	// 读取循环
	for {
		readCtx, cancel := withTimeoutIfSet(sc.ctx, s.ioTimeout)
		msgType, msg, err := sc.ws.Read(readCtx)
		cancel()
		if err != nil {
			if isExpectedCloseError(err) {
				return
			}
			log.Printf("server read error: %v", err)
			if s.erred != nil {
				s.erred(sc, err)
			}
			return
		}

		sc.markActive()
		switch msgType {
		case websocket.MessageBinary, websocket.MessageText:
			_ = s.stream.TryPut(serverMessage{conn: sc, data: msg})
		}
	}
}

func (s *Server) writeMessages(sc *serverConn) {
	defer func() {
		sc.send.Close()
		s.failQueuedWrites(sc, stream.ErrStreamClosed)
		sc.closeWithStatus(websocket.StatusNormalClosure, "writer exit")
	}()

	for {
		req, err := sc.send.Get(sc.ctx)
		if err != nil {
			return
		}

		writeCtx, cancel := withTimeoutIfSet(req.ctx, s.ioTimeout)
		err = sc.ws.Write(writeCtx, websocket.MessageBinary, req.data)
		cancel()

		req.resolve(err)

		if err != nil {
			if isExpectedCloseError(err) {
				return
			}
			log.Printf("server write error: %v", err)
			if s.erred != nil {
				s.erred(sc, err)
			}
			return
		}

		sc.markActive()
	}
}

func (s *Server) failQueuedWrites(sc *serverConn, err error) {
	for {
		req, getErr := sc.send.Get(context.Background())
		if getErr != nil {
			return
		}
		req.resolve(err)
	}
}

// closeWithStatus 关闭连接。
func (sc *serverConn) closeWithStatus(status websocket.StatusCode, reason string) {
	sc.onceClose.Do(func() {
		sc.cancel()
		sc.send.Close()
		_ = sc.ws.Close(status, reason)
	})
}

// Get 从服务端流中获取一条消息（阻塞）。
func (s *Server) Get(ctx context.Context) (*serverConn, []byte, error) {
	msg, err := s.stream.Get(ctx)
	if err != nil {
		return nil, nil, err
	}
	return msg.conn, msg.data, nil
}

// Put 发送一条二进制消息到指定的连接。
func (s *Server) Put(ctx context.Context, sc *serverConn, data []byte) error {
	req := newSendRequest(ctx, data)
	if err := sc.send.Put(ctx, req); err != nil {
		return err
	}
	return req.waitResult(ctx)
}

// Broadcast 发送数据到所有连接（有并发限制）。
func (s *Server) Broadcast(ctx context.Context, data []byte) {
	s.mu.RLock()
	conns := make([]*serverConn, 0, len(s.conns))
	for _, c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.RUnlock()

	sem := make(chan struct{}, s.broadcastConcurrency)
	var wg sync.WaitGroup
	for _, c := range conns {
		wg.Add(1)
		sem <- struct{}{}
		go func(cc *serverConn) {
			defer func() {
				<-sem
				wg.Done()
			}()
			_ = s.Put(ctx, cc, data)
		}(c)
	}
	wg.Wait()
}

// ConnCount 返回当前的连接数。
func (s *Server) ConnCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.conns)
}

// Close 关闭所有连接并等待它们完成。
func (s *Server) Close() {
	s.closeOnce.Do(func() {
		s.isClosed.Store(true)
		close(s.done)

		s.mu.Lock()
		conns := make([]*serverConn, 0, len(s.conns))
		for _, c := range s.conns {
			conns = append(conns, c)
		}
		s.mu.Unlock()

		var wg sync.WaitGroup
		for _, c := range conns {
			wg.Add(1)
			go func(cc *serverConn) {
				defer wg.Done()
				cc.closeWithStatus(websocket.StatusNormalClosure, "server shutdown")
			}(c)
		}
		wg.Wait()
		s.wg.Wait()
		s.stream.Close()
	})
}
