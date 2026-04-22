package connectionmanager

import (
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/e4623/go-rabbitmq/internal/dispatcher"
	"github.com/e4623/go-rabbitmq/internal/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

// errConnectionManagerClosed signals reconnectLoop that the caller has
// closed the manager and the loop must terminate rather than keep dialing
// fresh connections — otherwise a reconnect that wins the race against
// Close() would leave a live zombie connection (and any channels rebuilt
// on it) outliving the user's shutdown request.
var errConnectionManagerClosed = errors.New("connection manager closed")

// ConnectionManager -
type ConnectionManager struct {
	logger              logger.Logger
	resolver            Resolver
	connection          *amqp.Connection
	amqpConfig          amqp.Config
	connectionMu        *sync.RWMutex
	ReconnectInterval   time.Duration
	reconnectionCount   uint
	reconnectionCountMu *sync.Mutex
	dispatcher          *dispatcher.Dispatcher
	preConnectionFunc   func() // used for reconnection, this is a func will always be called before making connection to AMQP, this value should never be nil because when pass a nil func will automatically create an empty func
	isClosed            bool   // protected by connectionMu
}

type Resolver interface {
	Resolve() ([]string, error)
}

// dial will attempt to connect to the a list of urls in the order they are
// given.
func dial(log logger.Logger, resolver Resolver, conf amqp.Config) (*amqp.Connection, error) {
	urls, err := resolver.Resolve()
	if err != nil {
		return nil, fmt.Errorf("error resolving amqp server urls: %w", err)
	}

	var errs []error
	for _, url := range urls {
		conn, err := amqp.DialConfig(url, amqp.Config(conf))
		if err == nil {
			return conn, err
		}
		log.Warnf("failed to connect to amqp server %s: %v", maskPassword(url), err)
		errs = append(errs, err)
	}
	return nil, errors.Join(errs...)
}

func maskPassword(urlToMask string) string {
	parsedUrl, _ := url.Parse(urlToMask)
	return parsedUrl.Redacted()
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(resolver Resolver, conf amqp.Config, log logger.Logger, reconnectInterval time.Duration, preConnectionFunc func()) (*ConnectionManager, error) {

	if preConnectionFunc == nil { // check if preConnectionFunc pass nil value, create an empty func as default
		// should NEVER reach here because we load the default
		preConnectionFunc = func() {}
	}

	preConnectionFunc()
	conn, err := dial(log, resolver, amqp.Config(conf))
	if err != nil {
		return nil, err
	}

	connManager := ConnectionManager{
		logger:              log,
		resolver:            resolver,
		connection:          conn,
		amqpConfig:          conf,
		connectionMu:        &sync.RWMutex{},
		ReconnectInterval:   reconnectInterval,
		reconnectionCount:   0,
		reconnectionCountMu: &sync.Mutex{},
		dispatcher:          dispatcher.NewDispatcher(),
		preConnectionFunc:   preConnectionFunc,
	}
	go connManager.startNotifyClose()
	return &connManager, nil
}

// Close safely closes the current channel and connection
func (connManager *ConnectionManager) Close() error {
	connManager.logger.Infof("closing connection manager...")
	connManager.connectionMu.Lock()
	defer connManager.connectionMu.Unlock()

	connManager.isClosed = true

	err := connManager.connection.Close()
	if err != nil {
		return err
	}
	return nil
}

// NotifyReconnect adds a new subscriber that will receive error messages whenever
// the connection manager has successfully reconnected to the server
func (connManager *ConnectionManager) NotifyReconnect() (<-chan error, chan<- struct{}) {
	return connManager.dispatcher.AddSubscriber()
}

// CheckoutConnection -
func (connManager *ConnectionManager) CheckoutConnection() *amqp.Connection {
	connManager.connectionMu.RLock()
	return connManager.connection
}

// CheckinConnection -
func (connManager *ConnectionManager) CheckinConnection() {
	connManager.connectionMu.RUnlock()
}

// startNotifyCancelOrClosed listens on the channel's cancelled and closed
// notifiers. When it detects a problem, it attempts to reconnect.
// Once reconnected, it sends an error back on the manager's notifyCancelOrClose
// channel
func (connManager *ConnectionManager) startNotifyClose() {
	connManager.connectionMu.RLock()
	conn := connManager.connection
	connManager.connectionMu.RUnlock()

	notifyCloseChan := conn.NotifyClose(make(chan *amqp.Error, 1))

	// Guard against the race where conn died between reconnect() swapping
	// it in and us registering NotifyClose above. amqp091-go's NotifyClose
	// closes the passed chan silently when the connection is already closed
	// (noNotify branch), so we would read nil from notifyCloseChan, log
	// "closed gracefully", and exit — leaving the library with no active
	// supervisor on the connection and all ChannelManagers retrying forever.
	if conn.IsClosed() {
		connManager.logger.Errorf("connection observed closed immediately after reconnect, re-reconnecting")
		if loopErr := connManager.reconnectLoop(); loopErr != nil {
			return
		}
		connManager.logger.Warnf("successfully reconnected to amqp server")
		connManager.dispatcher.Dispatch(amqp.ErrClosed)
		return
	}

	err := <-notifyCloseChan
	if err != nil {
		connManager.logger.Errorf("attempting to reconnect to amqp server after connection close with error: %v", err)
		if loopErr := connManager.reconnectLoop(); loopErr != nil {
			return
		}
		connManager.logger.Warnf("successfully reconnected to amqp server")
		connManager.dispatcher.Dispatch(err)
	}
	if err == nil {
		connManager.logger.Infof("amqp connection closed gracefully")
	}
}

// GetReconnectionCount -
func (connManager *ConnectionManager) GetReconnectionCount() uint {
	connManager.reconnectionCountMu.Lock()
	defer connManager.reconnectionCountMu.Unlock()
	return connManager.reconnectionCount
}

func (connManager *ConnectionManager) incrementReconnectionCount() {
	connManager.reconnectionCountMu.Lock()
	defer connManager.reconnectionCountMu.Unlock()
	connManager.reconnectionCount++
}

// reconnectLoop continuously attempts to reconnect. It returns nil on a
// successful rebuild or errConnectionManagerClosed if the manager was closed
// while the loop was running — callers must not dispatch a reconnect signal
// in the latter case, since no new connection exists.
func (connManager *ConnectionManager) reconnectLoop() error {
	for {
		connManager.logger.Infof("waiting %s seconds to attempt to reconnect to amqp server", connManager.ReconnectInterval)
		time.Sleep(connManager.ReconnectInterval)
		err := connManager.reconnect()
		if errors.Is(err, errConnectionManagerClosed) {
			connManager.logger.Infof("connection manager closed, abandoning reconnect loop")
			return err
		}
		if err != nil {
			connManager.logger.Errorf("error reconnecting to amqp server: %v", err)
			continue
		}
		connManager.incrementReconnectionCount()
		go connManager.startNotifyClose()
		return nil
	}
}

// reconnect safely closes the current channel and obtains a new one
func (connManager *ConnectionManager) reconnect() error {
	connManager.connectionMu.Lock()
	defer connManager.connectionMu.Unlock()

	// The caller closed us while we were sleeping in reconnectLoop. Do not
	// dial a fresh connection — otherwise the new connection (and any
	// channels rebuilt on it) would outlive the user's shutdown request.
	if connManager.isClosed {
		return errConnectionManagerClosed
	}

	connManager.preConnectionFunc()
	conn, err := dial(connManager.logger, connManager.resolver, amqp.Config(connManager.amqpConfig))
	if err != nil {
		return err
	}

	if err = connManager.connection.Close(); err != nil {
		connManager.logger.Warnf("error closing connection while reconnecting: %v", err)
	}

	connManager.connection = conn
	return nil
}

// IsClosed checks if the connection is closed
func (connManager *ConnectionManager) IsClosed() bool {
	connManager.connectionMu.Lock()
	defer connManager.connectionMu.Unlock()

	return connManager.connection.IsClosed()
}

// IsManagerClosed reports whether the caller has closed the ConnectionManager
// itself (as opposed to the underlying amqp connection being dead but
// recoverable). ChannelManagers consult this to distinguish "connection
// down, keep retrying" from "manager shut down, abandon reconnect loop" —
// otherwise a user-initiated Conn.Close while a channel's reconnectLoop is
// sleeping would leave that loop retrying forever.
func (connManager *ConnectionManager) IsManagerClosed() bool {
	connManager.connectionMu.RLock()
	defer connManager.connectionMu.RUnlock()
	return connManager.isClosed
}
