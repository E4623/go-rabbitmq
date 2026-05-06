package channelmanager

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/e4623/go-rabbitmq/internal/connectionmanager"
	"github.com/e4623/go-rabbitmq/internal/dispatcher"
	"github.com/e4623/go-rabbitmq/internal/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

// errChannelManagerClosed signals reconnectLoop that the manager has been
// closed by the caller and the loop must terminate rather than keep trying
// to rebuild a channel — otherwise a reconnect that wins the race against
// Close() would register a new consumer after the user asked us to stop.
var errChannelManagerClosed = errors.New("channel manager closed")

// livenessProbeTimeout caps how long the Qos-based liveness probe in
// reconnect() can hold channelMu before bailing out. amqp091-go's call()
// has no timeout, so without this, a network-partitioned (TCP-up, AMQP-dead)
// connection could wedge reconnect for up to ~20s — blocking every
// publisher/consumer on channelMu.RLock for that duration.
const livenessProbeTimeout = 10 * time.Second

// Options bundles ChannelManager configuration. Optional fields default to
// nil/0 — the manager treats those as "use sensible defaults".
type Options struct {
	Logger            logger.Logger
	ReconnectInterval time.Duration
	Backoff           func(attempt int) time.Duration // nil = use ReconnectInterval (fixed)
	OnChannelLost     func(err error)                 // nil = no-op
}

// ChannelManager -
type ChannelManager struct {
	logger              logger.Logger
	channel             *amqp.Channel
	connManager         *connectionmanager.ConnectionManager
	channelMu           *sync.RWMutex
	reconnectInterval   time.Duration
	backoff             func(attempt int) time.Duration
	reconnectionCount   uint
	reconnectionCountMu *sync.Mutex
	dispatcher          *dispatcher.Dispatcher
	onChannelLost       func(err error)
	isClosed            bool // protected by channelMu
}

// NewChannelManager creates a new channel manager.
func NewChannelManager(connManager *connectionmanager.ConnectionManager, opts Options) (*ChannelManager, error) {
	ch, err := getNewChannel(connManager)
	if err != nil {
		return nil, err
	}

	chanManager := ChannelManager{
		logger:              opts.Logger,
		connManager:         connManager,
		channel:             ch,
		channelMu:           &sync.RWMutex{},
		reconnectInterval:   opts.ReconnectInterval,
		backoff:             opts.Backoff,
		reconnectionCount:   0,
		reconnectionCountMu: &sync.Mutex{},
		dispatcher:          dispatcher.NewDispatcher(),
		onChannelLost:       opts.OnChannelLost,
	}
	go chanManager.startNotifyCancelOrClosed()
	return &chanManager, nil
}

// nextBackoff returns the delay before the given retry attempt (1-based).
func (chanManager *ChannelManager) nextBackoff(attempt int) time.Duration {
	if chanManager.backoff != nil {
		return chanManager.backoff(attempt)
	}
	return chanManager.reconnectInterval
}

func getNewChannel(connManager *connectionmanager.ConnectionManager) (*amqp.Channel, error) {
	conn := connManager.CheckoutConnection()
	defer connManager.CheckinConnection()

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// startNotifyCancelOrClosed listens on the channel's cancelled and closed
// notifiers. When it detects a problem, it attempts to reconnect.
// Once reconnected, it sends an error back on the manager's notifyCancelOrClose
// channel
func (chanManager *ChannelManager) startNotifyCancelOrClosed() {
	chanManager.channelMu.RLock()
	ch := chanManager.channel
	chanManager.channelMu.RUnlock()

	notifyCloseChan := ch.NotifyClose(make(chan *amqp.Error, 1))
	notifyCancelChan := ch.NotifyCancel(make(chan string, 1))

	// Guard against the race where ch died between reconnect() swapping it
	// in and us registering NotifyClose above. amqp091-go's NotifyClose
	// closes the passed chan silently when the channel is already closed
	// (noNotify branch), so we would read nil from notifyCloseChan and
	// exit as "graceful" — leaving a permanent ghost consumer.
	if ch.IsClosed() {
		chanManager.logger.Errorf("channel observed closed immediately after reconnect, re-reconnecting")
		if chanManager.onChannelLost != nil {
			chanManager.onChannelLost(amqp.ErrClosed)
		}
		if loopErr := chanManager.reconnectLoop(); loopErr != nil {
			return
		}
		chanManager.logger.Warnf("successfully reconnected to amqp server")
		chanManager.dispatcher.Dispatch(amqp.ErrClosed)
		return
	}

	select {
	case err := <-notifyCloseChan:
		if err != nil {
			chanManager.logger.Errorf("attempting to reconnect to amqp server after close with error: %v", err)
			if chanManager.onChannelLost != nil {
				chanManager.onChannelLost(err)
			}
			if loopErr := chanManager.reconnectLoop(); loopErr != nil {
				return
			}
			chanManager.logger.Warnf("successfully reconnected to amqp server")
			chanManager.dispatcher.Dispatch(err)
		}
		if err == nil {
			chanManager.logger.Infof("amqp channel closed gracefully")
			// A graceful close means either the caller closed us, or the
			// owning ConnectionManager was closed (which closes all channels
			// as part of shutdown). In either case, shut down the dispatcher
			// so subscribers blocked on reconnectErrCh can exit — otherwise
			// Consumer.Run / Publisher recovery would hang forever waiting
			// for a signal that will never come.
			chanManager.channelMu.RLock()
			managerClosed := chanManager.isClosed
			chanManager.channelMu.RUnlock()
			if managerClosed || chanManager.connManager.IsManagerClosed() {
				chanManager.dispatcher.Shutdown()
			}
		}
	case err := <-notifyCancelChan:
		chanManager.logger.Errorf("attempting to reconnect to amqp server after cancel with error: %s", err)
		cancelErr := errors.New(err)
		if chanManager.onChannelLost != nil {
			chanManager.onChannelLost(cancelErr)
		}
		if loopErr := chanManager.reconnectLoop(); loopErr != nil {
			return
		}
		chanManager.logger.Warnf("successfully reconnected to amqp server after cancel")
		chanManager.dispatcher.Dispatch(cancelErr)
	}
}

// GetReconnectionCount -
func (chanManager *ChannelManager) GetReconnectionCount() uint {
	chanManager.reconnectionCountMu.Lock()
	defer chanManager.reconnectionCountMu.Unlock()
	return chanManager.reconnectionCount
}

func (chanManager *ChannelManager) incrementReconnectionCount() {
	chanManager.reconnectionCountMu.Lock()
	defer chanManager.reconnectionCountMu.Unlock()
	chanManager.reconnectionCount++
}

// reconnectLoop continuously attempts to reconnect. Returns nil on success
// or errChannelManagerClosed when the manager itself or its parent
// ConnectionManager has been closed — callers must not dispatch a reconnect
// signal in the latter case, since no new channel exists.
func (chanManager *ChannelManager) reconnectLoop() error {
	for attempt := 1; ; attempt++ {
		delay := chanManager.nextBackoff(attempt)
		chanManager.logger.Infof("waiting %s before channel reconnect attempt %d", delay, attempt)
		time.Sleep(delay)

		err := chanManager.reconnect()
		if errors.Is(err, errChannelManagerClosed) {
			chanManager.logger.Infof("channel manager closed, abandoning reconnect loop")
			// Force-close dispatcher subscribers so owners blocked on
			// `for err := range reconnectErrCh` (Consumer.Run,
			// Publisher recovery goroutine) can exit cleanly.
			chanManager.dispatcher.Shutdown()
			return err
		}
		if err == nil {
			chanManager.incrementReconnectionCount()
			go chanManager.startNotifyCancelOrClosed()
			return nil
		}
		chanManager.logger.Errorf("error reconnecting to amqp server (attempt %d): %v", attempt, err)
	}
}

// reconnect safely closes the current channel and obtains a new one
func (chanManager *ChannelManager) reconnect() error {
	chanManager.channelMu.Lock()
	defer chanManager.channelMu.Unlock()

	// The caller closed us while we were sleeping in reconnectLoop. Do not
	// rebuild — otherwise a new channel (and any consumer registered on it
	// by the calling layer) would come up after the user asked us to stop.
	if chanManager.isClosed {
		return errChannelManagerClosed
	}

	// The ConnectionManager has been closed by the caller (e.g. Conn.Close()
	// without closing consumers first). Treat this as terminal: no point
	// looping forever trying to rebuild on a connection that will never be
	// restored.
	if chanManager.connManager.IsManagerClosed() {
		return errChannelManagerClosed
	}

	// Refuse to rebuild on a dead connection. If the underlying connection is
	// closed, ConnectionManager is concurrently reconnecting; racing it would
	// leave us with a channel on the stale connection — the server never sees
	// the consumer while the client thinks it succeeded (ghost consumer).
	// Returning an error lets reconnectLoop wait and retry after the connection
	// has been replaced.
	if chanManager.connManager.IsClosed() {
		return errors.New("underlying connection is closed, waiting for connection manager to reconnect")
	}

	newChannel, err := getNewChannel(chanManager.connManager)
	if err != nil {
		return err
	}

	// Liveness probe: a sync RPC round-trip against the new channel. If the
	// underlying connection was half-dead when we opened the channel (broker
	// sent channel.close before the client's connection-close notification
	// propagated), this call fails here rather than letting us register a
	// ghost consumer. Qos(0,0,false) is a no-op from the broker's view.
	//
	// The probe runs in a goroutine with a timeout because amqp091-go's
	// Channel.call() has no timeout — on a TCP-up-but-AMQP-dead connection
	// it blocks for the heartbeat interval. Without the timeout we'd hold
	// channelMu and block all I/O for up to ~20s.
	probeErrCh := make(chan error, 1)
	go func() {
		probeErrCh <- newChannel.Qos(0, 0, false)
	}()
	select {
	case err := <-probeErrCh:
		if err != nil {
			_ = newChannel.Close()
			return fmt.Errorf("new channel failed liveness probe: %w", err)
		}
	case <-time.After(livenessProbeTimeout):
		// Closing the channel unblocks the pending Qos so its goroutine
		// can exit via the buffered chan — no goroutine leak.
		_ = newChannel.Close()
		return fmt.Errorf("new channel liveness probe timed out after %s", livenessProbeTimeout)
	}

	if err = chanManager.channel.Close(); err != nil {
		chanManager.logger.Warnf("error closing channel while reconnecting: %v", err)
	}

	chanManager.channel = newChannel
	return nil
}

// Close safely closes the current channel and connection
func (chanManager *ChannelManager) Close() error {
	chanManager.logger.Infof("closing channel manager...")
	chanManager.channelMu.Lock()
	chanManager.isClosed = true
	err := chanManager.channel.Close()
	chanManager.channelMu.Unlock()

	// Shutdown the dispatcher so any Consumer.Run / Publisher recovery
	// goroutines blocked on reconnectErrCh can exit. Idempotent — safe to
	// call even if reconnectLoop already shut it down.
	chanManager.dispatcher.Shutdown()

	if err != nil {
		return err
	}
	return nil
}

// NotifyReconnect adds a new subscriber that will receive error messages whenever
// the connection manager has successfully reconnect to the server
func (chanManager *ChannelManager) NotifyReconnect() (<-chan error, chan<- struct{}) {
	return chanManager.dispatcher.AddSubscriber()
}
