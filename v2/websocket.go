package binance

import (
	"context"
	"time"

	"nhooyr.io/websocket"
)

// WsHandler handle raw websocket message
type WsHandler func(message []byte)

// ErrHandler handles errors
type ErrHandler func(err error)

// WsConfig webservice configuration
type WsConfig struct {
	Endpoint string
}

func newWsConfig(endpoint string) *WsConfig {
	return &WsConfig{
		Endpoint: endpoint,
	}
}

func wsServeFunc(cfg *WsConfig, handler WsHandler, errHandler ErrHandler) (doneC, stopC chan struct{}, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	c, _, err := websocket.Dial(ctx, cfg.Endpoint, nil)
	if err != nil {
		cancel()
		return nil, nil, err
	}
	c.SetReadLimit(655350)
	doneC = make(chan struct{})
	stopC = make(chan struct{})
	receivedDataC := make(chan bool)
	go func() {
		// This function will exit either on error from
		// websocket.Conn.ReadMessage or when the stopC channel is
		// closed by the client.
		defer close(doneC)
		defer cancel()
		if WebsocketKeepalive {
			go keepAlive(ctx, c, WebsocketTimeout)
		}
		// Wait for the stopC channel to be closed.  We do that in a
		// separate goroutine because ReadMessage is a blocking
		// operation.
		silent := false
		close := false
		reconnect := false
		go func() {
			for {
				select {
				case <-receivedDataC:
					//If we received data then we do nothing
				case <-time.After(2 * time.Second):
					//If we reach this case we need to perform the reconnect. This means we haven't received a message for 2 seconds.
					close = true
					reconnect = true
				case <-stopC:
					silent = true
					close = true
				case <-doneC:
					close = true
				}
				if close {
					_ = c.Close(websocket.StatusNormalClosure, "normal closure")
				}
				if reconnect {
					go wsServeFunc(cfg, handler, errHandler)
					return
				}
			}
		}()
		for {
			_, message, readErr := c.Read(ctx)
			if readErr != nil {
				if !silent {
					errHandler(readErr)
				}
				return
			}
			receivedDataC <- true
			handler(message)
		}
	}()
	return
}

var wsServe = wsServeFunc

func keepAlive(ctx context.Context, c *websocket.Conn, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		err := c.Ping(ctx)
		if err != nil {
			return
		}

		t.Reset(d)
	}
}
