package wss

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/coder/websocket"
)

type writeRequest struct {
	ctx    context.Context
	data   []byte
	result chan error
}

func newSendRequest(ctx context.Context, data []byte) writeRequest {
	dataCopy := append([]byte(nil), data...)
	return writeRequest{
		ctx:    ctx,
		data:   dataCopy,
		result: make(chan error, 1),
	}
}

func (r writeRequest) resolve(err error) {
	r.result <- err
}

func (r writeRequest) waitResult(ctx context.Context) error {
	select {
	case err := <-r.result:
		return err
	default:
	}

	select {
	case err := <-r.result:
		return err
	case <-ctx.Done():
		select {
		case err := <-r.result:
			return err
		default:
			return ctx.Err()
		}
	}
}

func isExpectedCloseError(err error) bool {
	if err == nil {
		return false
	}

	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure ||
		status == websocket.StatusGoingAway ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, net.ErrClosed)
}
