package wss

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"stream"
)

func TestServerCloseIsIdempotentAndRejectsNewConnections(t *testing.T) {
	srv := NewServer()
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, _, err := websocket.Dial(ctx, testWSURL(ts.URL), nil)
	if err != nil {
		t.Fatalf("dial before close failed: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Close(websocket.StatusNormalClosure, "test cleanup")
	})

	waitForConnCount(t, srv, 1)

	srv.Close()
	srv.Close()

	waitForConnCount(t, srv, 0)

	_, resp, err := websocket.Dial(ctx, testWSURL(ts.URL), nil)
	if err == nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		t.Fatal("expected dial after close to fail")
	}
	if resp == nil {
		t.Fatalf("expected HTTP response after close, got nil")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, resp.StatusCode)
	}
}

func TestZeroIOTimeoutStillAllowsEcho(t *testing.T) {
	srv := NewServer(WithServerIOTimeout(0))
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)

	echoErr := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		conn, msg, err := srv.Get(ctx)
		if err != nil {
			echoErr <- err
			return
		}
		echoErr <- srv.Put(ctx, conn, msg)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := NewClient(ctx, testWSURL(ts.URL), WithClientIOTimeout(0))
	if err != nil {
		t.Fatalf("new client failed: %v", err)
	}
	t.Cleanup(client.Close)

	payload := []byte("hello")
	if err := client.Put(ctx, payload); err != nil {
		t.Fatalf("client put failed with zero timeout: %v", err)
	}

	got, err := client.Get(ctx)
	if err != nil {
		t.Fatalf("client get failed with zero timeout: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("unexpected echo payload: got %q want %q", got, payload)
	}

	select {
	case err := <-echoErr:
		if err != nil {
			t.Fatalf("server echo failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server echo")
	}
}

func TestServerBroadcastWithNonPositiveConcurrencyStillSends(t *testing.T) {
	srv := NewServer(WithServerBroadcastConcurrency(0))
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, _, err := websocket.Dial(ctx, testWSURL(ts.URL), nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Close(websocket.StatusNormalClosure, "test cleanup")
	})

	waitForConnCount(t, srv, 1)

	done := make(chan struct{})
	go func() {
		srv.Broadcast(context.Background(), []byte("broadcast"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("broadcast blocked with non-positive concurrency")
	}

	msgCtx, msgCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer msgCancel()

	msgType, payload, err := client.Read(msgCtx)
	if err != nil {
		t.Fatalf("client read failed: %v", err)
	}
	if msgType != websocket.MessageBinary {
		t.Fatalf("unexpected message type: %v", msgType)
	}
	if !bytes.Equal(payload, []byte("broadcast")) {
		t.Fatalf("unexpected broadcast payload: got %q", payload)
	}
}

func TestClientPutAfterCloseReturnsStreamClosed(t *testing.T) {
	srv := NewServer()
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, err := NewClient(ctx, testWSURL(ts.URL))
	if err != nil {
		t.Fatalf("new client failed: %v", err)
	}

	client.Close()

	err = client.Put(context.Background(), []byte("closed"))
	if !errors.Is(err, stream.ErrStreamClosed) {
		t.Fatalf("expected stream.ErrStreamClosed, got %v", err)
	}
}

func TestServerPutAfterCloseReturnsStreamClosed(t *testing.T) {
	connCh := make(chan *serverConn, 1)
	srv := NewServer(WithServerOpened(func(conn *serverConn) {
		connCh <- conn
	}))
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, _, err := websocket.Dial(ctx, testWSURL(ts.URL), nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	t.Cleanup(func() {
		_ = client.Close(websocket.StatusNormalClosure, "test cleanup")
	})

	var conn *serverConn
	select {
	case conn = <-connCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for opened connection")
	}

	srv.Close()

	err = srv.Put(context.Background(), conn, []byte("closed"))
	if !errors.Is(err, stream.ErrStreamClosed) {
		t.Fatalf("expected stream.ErrStreamClosed, got %v", err)
	}
}

func waitForConnCount(t *testing.T, srv *Server, want int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if srv.ConnCount() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("conn count mismatch: got %d want %d", srv.ConnCount(), want)
}

func testWSURL(httpURL string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http")
}
