package rabbitmq

import "time"

// ConnectionOptions are used to describe how a new consumer will be created.
type ConnectionOptions struct {
	ReconnectInterval time.Duration
	Logger            Logger
	Config            Config
	PreConnectionFunc func()
}

// getDefaultConnectionOptions describes the options that will be used when a value isn't provided
func getDefaultConnectionOptions() ConnectionOptions {
	return ConnectionOptions{
		ReconnectInterval: time.Second * 5,
		Logger:            stdDebugLogger{},
		Config:            Config{},
		PreConnectionFunc: func() {},
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
