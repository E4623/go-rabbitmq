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

// ErrReconnectAttemptsExhausted is returned to NotifyReconnect subscribers
// when MaxReconnectAttempts > 0 and the loop has exhausted retries without
// success. The dispatcher is then shut down so subscribers exit cleanly.
var ErrReconnectAttemptsExhausted = errors.New("reconnect attempts exhausted")

// Options bundles the configuration for a ConnectionManager. Internal
// constructor parameters were getting unwieldy; this struct keeps them
// named and lets callers leave optional hooks zero.
type Options struct {
	Resolver             Resolver
	AmqpConfig           amqp.Config
	Logger               logger.Logger
	ReconnectInterval    time.Duration
	Backoff              func(attempt int) time.Duration // nil = use ReconnectInterval (fixed)
	MaxReconnectAttempts int                             // 0 = unbounded
	PreConnectionFunc    func()                          // nil = noop
	OnConnectionLost     func(err error)                 // nil = no-op
}

// ConnectionManager -
type ConnectionManager struct {
	logger               logger.Logger
	resolver             Resolver
	connection           *amqp.Connection
	amqpConfig           amqp.Config
	connectionMu         *sync.RWMutex
	ReconnectInterval    time.Duration
	backoff              func(attempt int) time.Duration
	maxReconnectAttempts int
	reconnectionCount    uint
	reconnectionCountMu  *sync.Mutex
	dispatcher           *dispatcher.Dispatcher
	preConnectionFunc    func()           // never nil — defaulted to a no-op when empty
	onConnectionLost     func(err error)  // may be nil
	isClosed             bool             // protected by connectionMu
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

// NewConnectionManager creates a new connection manager.
func NewConnectionManager(opts Options) (*ConnectionManager, error) {
	if opts.PreConnectionFunc == nil {
		opts.PreConnectionFunc = func() {}
	}

	opts.PreConnectionFunc()
	conn, err := dial(opts.Logger, opts.Resolver, opts.AmqpConfig)
	if err != nil {
		return nil, err
	}

	connManager := ConnectionManager{
		logger:               opts.Logger,
		resolver:             opts.Resolver,
		connection:           conn,
		amqpConfig:           opts.AmqpConfig,
		connectionMu:         &sync.RWMutex{},
		ReconnectInterval:    opts.ReconnectInterval,
		backoff:              opts.Backoff,
		maxReconnectAttempts: opts.MaxReconnectAttempts,
		reconnectionCount:    0,
		reconnectionCountMu:  &sync.Mutex{},
		dispatcher:           dispatcher.NewDispatcher(),
		preConnectionFunc:    opts.PreConnectionFunc,
		onConnectionLost:     opts.OnConnectionLost,
	}
	go connManager.startNotifyClose()
	return &connManager, nil
}

// nextBackoff returns the delay before retry attempt `attempt` (1-based).
// Falls back to ReconnectInterval when no Backoff function was provided.
func (connManager *ConnectionManager) nextBackoff(attempt int) time.Duration {
	if connManager.backoff != nil {
		return connManager.backoff(attempt)
	}
	return connManager.ReconnectInterval
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
		if connManager.onConnectionLost != nil {
			connManager.onConnectionLost(amqp.ErrClosed)
		}
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
		if connManager.onConnectionLost != nil {
			connManager.onConnectionLost(err)
		}
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

// reconnectLoop continuously attempts to reconnect. Returns nil on success,
// errConnectionManagerClosed if the manager was closed mid-loop, or
// ErrReconnectAttemptsExhausted if MaxReconnectAttempts was set and reached
// without a successful reconnect. Callers must not dispatch a reconnect
// signal on terminal errors — no new connection exists.
func (connManager *ConnectionManager) reconnectLoop() error {
	for attempt := 1; ; attempt++ {
		delay := connManager.nextBackoff(attempt)
		connManager.logger.Infof("waiting %s before reconnect attempt %d", delay, attempt)
		time.Sleep(delay)

		err := connManager.reconnect()
		if errors.Is(err, errConnectionManagerClosed) {
			connManager.logger.Infof("connection manager closed, abandoning reconnect loop")
			return err
		}
		if err == nil {
			connManager.incrementReconnectionCount()
			go connManager.startNotifyClose()
			return nil
		}

		connManager.logger.Errorf("error reconnecting to amqp server (attempt %d): %v", attempt, err)
		if connManager.maxReconnectAttempts > 0 && attempt >= connManager.maxReconnectAttempts {
			connManager.logger.Errorf("exhausted %d reconnect attempts, giving up", connManager.maxReconnectAttempts)
			// Mark closed so children (ChannelManagers) see IsManagerClosed
			// and abandon their own loops, then shut down subscribers so
			// owners blocked on reconnectErrCh exit.
			connManager.connectionMu.Lock()
			connManager.isClosed = true
			connManager.connectionMu.Unlock()
			connManager.dispatcher.Shutdown()
			return ErrReconnectAttemptsExhausted
		}
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
