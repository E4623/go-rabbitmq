package rabbitmq

import "time"

// ConnectionOptions are used to describe how a new consumer will be created.
type ConnectionOptions struct {
	ReconnectInterval time.Duration
	// Backoff, when non-nil, returns the delay before each reconnect
	// attempt (1-based). When nil the library uses ReconnectInterval as a
	// fixed delay. Use this to implement exponential backoff with jitter
	// at production scale to avoid the thundering-herd effect when many
	// clients reconnect to a recovering broker.
	Backoff func(attempt int) time.Duration
	// MaxReconnectAttempts caps reconnect attempts. 0 (the default) means
	// retry forever. When set and exhausted, the connection manager closes
	// itself and any subscribers (Consumer.Run, Publisher recovery loops)
	// receive the closed-channel signal and exit.
	MaxReconnectAttempts int
	Logger               Logger
	Config               Config
	PreConnectionFunc    func()
	// OnReconnect, if non-nil, is called synchronously after the connection
	// has been successfully rebuilt. The argument is the error that triggered
	// the reconnect cycle. The hook is fired exactly once per successful
	// reconnect, never on individual failed retry attempts. If you need
	// non-blocking behavior, spawn a goroutine inside the callback.
	OnReconnect func(err error)
	// OnConnectionLost, if non-nil, is called synchronously the moment the
	// underlying AMQP connection is observed dead, before any reconnect
	// attempt begins. Use this for fast alerting / readiness toggling
	// independent of reconnect outcome.
	OnConnectionLost func(err error)
}

// getDefaultConnectionOptions describes the options that will be used when a value isn't provided
func getDefaultConnectionOptions() ConnectionOptions {
	return ConnectionOptions{
		ReconnectInterval:    time.Second * 5,
		Backoff:              nil,
		MaxReconnectAttempts: 0,
		Logger:               stdDebugLogger{},
		Config:               Config{},
		PreConnectionFunc:    func() {},
		OnReconnect:          nil,
		OnConnectionLost:     nil,
	}
}

// WithConnectionOptionsReconnectInterval sets the reconnection interval
func WithConnectionOptionsReconnectInterval(interval time.Duration) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.ReconnectInterval = interval
	}
}

// WithConnectionOptionsLogging sets logging to true on the consumer options
// and sets the
func WithConnectionOptionsLogging(options *ConnectionOptions) {
	options.Logger = stdDebugLogger{}
}

// WithConnectionOptionsLogger sets logging to true on the consumer options
// and sets the
func WithConnectionOptionsLogger(log Logger) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.Logger = log
	}
}

// WithConnectionOptionsConfig sets the Config used in the connection
func WithConnectionOptionsConfig(cfg Config) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.Config = cfg
	}
}

// WithConnectionOptionsPreConnectionFunc sets a function to be called before dial or open connection to AMQP
func WithConnectionOptionsPreConnectionFunc(preConnFunc func()) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		if preConnFunc == nil {
			preConnFunc = func() {}
		}
		options.PreConnectionFunc = preConnFunc
	}
}

// WithConnectionOptionsOnReconnect registers a callback that fires
// synchronously after the connection has been successfully rebuilt. The
// argument is the error that triggered the reconnect cycle. The hook is
// fired exactly once per successful reconnect, not on each failed retry.
// To run application work asynchronously, spawn a goroutine inside the
// callback.
func WithConnectionOptionsOnReconnect(fn func(err error)) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.OnReconnect = fn
	}
}

// WithConnectionOptionsOnConnectionLost registers a callback that fires
// synchronously the moment the underlying AMQP connection is observed
// dead, before any reconnect attempt begins. Useful for fast alerting,
// readiness probe toggling, or upstream backpressure. The argument is the
// close error reported by the broker / network.
func WithConnectionOptionsOnConnectionLost(fn func(err error)) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.OnConnectionLost = fn
	}
}

// WithConnectionOptionsBackoff sets a custom backoff function returning the
// delay before each reconnect attempt (1-based). When unset, reconnects use
// a fixed ReconnectInterval. Use this to implement exponential backoff with
// jitter at production scale.
func WithConnectionOptionsBackoff(fn func(attempt int) time.Duration) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.Backoff = fn
	}
}

// WithConnectionOptionsMaxReconnectAttempts caps how many reconnect attempts
// the library will make before giving up. 0 (the default) means retry
// forever. When the cap is reached, subscribers (Consumer.Run, Publisher
// recovery loops) receive the closed-channel signal and exit.
func WithConnectionOptionsMaxReconnectAttempts(n int) func(options *ConnectionOptions) {
	return func(options *ConnectionOptions) {
		options.MaxReconnectAttempts = n
	}
}
