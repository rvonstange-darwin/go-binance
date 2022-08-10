package binance

import (
	"context"
	"time"

	"nhooyr.io/websocket"
)

// WsHandler handle raw websocket message
type WsHandler func(message []byte, connectionId int)

// ErrHandler handles errors
type ErrHandler func(err error, message string)

// WsConfig webservice configuration
type WsConfig struct {
	Endpoint string
}

func newWsConfig(endpoint string) *WsConfig {
	return &WsConfig{
		Endpoint: endpoint,
	}
}

const MISSING_MARKET_DATA_THRESHOLD time.Duration = 2 * time.Second

type RestartChannel struct {
	Id     int
	Status bool
}

type WsServeParams struct {
	cfg          *WsConfig
	handler      WsHandler
	errHandler   ErrHandler
	threshold    time.Duration
	connectionId int
}

func wsServeFunc(params WsServeParams) (doneC, stopC chan struct{}, restartC chan RestartChannel, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	c, _, err := websocket.Dial(ctx, params.cfg.Endpoint, nil)
	if err != nil {
		cancel()
		return nil, nil, nil, err
	}
	c.SetReadLimit(655350)
	doneC = make(chan struct{})
	stopC = make(chan struct{})
	restartC = make(chan RestartChannel)
	receivedDataC := make(chan bool)
	go func() {
		// This function will exit either on error from
		// websocket.Conn.ReadMessage or when the stopC channel is
		// closed by the client.
		defer close(doneC)
		defer close(receivedDataC)
		defer cancel()
		if WebsocketKeepalive {
			go keepAlive(ctx, c, WebsocketTimeout)
		}
		// Wait for the stopC channel to be closed.  We do that in a
		// separate goroutine because ReadMessage is a blocking
		// operation.
		restartThreshold := MISSING_MARKET_DATA_THRESHOLD
		if params.threshold != 0 {
			restartThreshold = params.threshold
		}
		silent := false
		close := false
		go func() {
			for {
				select {
				case <-receivedDataC:
					//If we received data then we do nothing
				case <-time.After(restartThreshold):
					//If we reach this case we need to perform the reconnect. This means we haven't received a message for 2 seconds.
					restartC <- RestartChannel{
						Id:     params.connectionId,
						Status: true,
					}
					close = true
				case <-stopC:
					silent = true
					close = true
				case <-doneC:
					close = true
				}
				if close {
					_ = c.Close(websocket.StatusNormalClosure, "normal closure")
					return
				}
			}
		}()
		for {
			_, message, readErr := c.Read(ctx)
			if readErr != nil {
				if !silent {
					params.errHandler(readErr, "WS Read Error occurred.")
				}
				return
			}
			if close {
				return
			}
			receivedDataC <- true
			params.handler(message, params.connectionId)
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
