# go-rabbitmq

A fork from https://github.com/wagslane/go-rabbitmq and modified for personal use case.

Below is description from `wagslane/go-rabbitmq`

A wrapper of [rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go) that provides reconnection logic and sane defaults. Hit the project with a star if you find it useful ⭐

Supported by [Boot.dev](https://boot.dev). If you'd like to learn about RabbitMQ and Go, you can check out [my course here](https://www.boot.dev/learn/learn-pub-sub).


## Motivation

[Streadway's AMQP](https://github.com/rabbitmq/amqp091-go) library is currently the most robust and well-supported Go client I'm aware of. It's a fantastic option and I recommend starting there and seeing if it fulfills your needs. Their project has made an effort to stay within the scope of the AMQP protocol, as such, no reconnection logic and few ease-of-use abstractions are provided.

### Goal

The goal with `go-rabbitmq` is to provide *most* (but not all) of the nitty-gritty functionality of Streadway's AMQP, but to make it easier to work with via a higher-level API. `go-rabbitmq` is also built specifically for Rabbit, not for the AMQP protocol. In particular, we want:

* Automatic reconnection
* Multithreaded consumers via a handler function
* Reasonable defaults
* Flow control handling
* TCP block handling

## ⚙️ Installation

Inside a Go module:

```bash
go get github.com/e4623/go-rabbitmq
```

## 🚀 Quick Start Consumer

Take note of the optional `options` parameters after the queue name. The *queue* will be declared automatically, but the *exchange* will not. You'll also *probably* want to bind to at least one routing key.

```go
conn, err := rabbitmq.NewConn(
	"amqp://guest:guest@localhost",
	rabbitmq.WithConnectionOptionsLogging,
)
if err != nil {
	log.Fatal(err)
}
defer conn.Close()

consumer, err := rabbitmq.NewConsumer(
	conn,
	"my_queue",
	rabbitmq.WithConsumerOptionsRoutingKey("my_routing_key"),
	rabbitmq.WithConsumerOptionsExchangeName("events"),
	rabbitmq.WithConsumerOptionsExchangeDeclare,
)
if err != nil {
	log.Fatal(err)
}
defer consumer.Close()

err = consumer.Run(func(d rabbitmq.Delivery) rabbitmq.Action {
	log.Printf("consumed: %v", string(d.Body))
	// rabbitmq.Ack, rabbitmq.NackDiscard, rabbitmq.NackRequeue
	return rabbitmq.Ack
})
if err != nil {
	log.Fatal(err)
}
```

## 🚀 Quick Start Publisher

The exchange is not declared by default, that's why I recommend using the following options.
```go
conn, err := rabbitmq.NewConn(
	"amqp://guest:guest@localhost",
	rabbitmq.WithConnectionOptionsLogging,
)
if err != nil {
	log.Fatal(err)
}
defer conn.Close()

publisher, err := rabbitmq.NewPublisher(
	conn,
	rabbitmq.WithPublisherOptionsLogging,
	rabbitmq.WithPublisherOptionsExchangeName("events"),
	rabbitmq.WithPublisherOptionsExchangeDeclare,
)
if err != nil {
	log.Fatal(err)
}
defer publisher.Close()

err = publisher.Publish(
	[]byte("hello, world"),
	[]string{"my_routing_key"},
	rabbitmq.WithPublishOptionsContentType("application/json"),
	rabbitmq.WithPublishOptionsExchange("events"),
)
if err != nil {
	log.Println(err)
}
```

## Other usage examples

See the [examples](examples) directory for more ideas.

## Options and configuring

* By default, queues are declared if they didn't already exist by new consumers
* By default, routing-key bindings are declared by consumers if you're using `WithConsumerOptionsRoutingKey`
* By default, exchanges are *not* declared by publishers or consumers if they don't already exist, hence `WithPublisherOptionsExchangeDeclare` and `WithConsumerOptionsExchangeDeclare`.

Read up on all the options in the GoDoc, there are quite a few of them. I try to pick sane and simple defaults.

## Reconnect lifecycle (v0.2.0+)

The library exposes lifecycle hooks and tunables so you can react to disconnects, take snapshots after recovery, and control retry behavior in production. Every hook is opt-in — leave it unset and behavior is unchanged.

### Hooks

| Layer | Fires when... | Option |
|---|---|---|
| Connection | The AMQP connection drops, before any retry | `WithConnectionOptionsOnConnectionLost(fn)` |
| Connection | The connection has been successfully rebuilt | `WithConnectionOptionsOnReconnect(fn)` |
| Consumer | The consumer's channel dies, before any retry | `WithConsumerOptionsOnChannelLost(fn)` |
| Consumer | The consumer is fully re-registered on a fresh channel | `WithConsumerOptionsOnReconnect(fn)` |
| Publisher | The publisher's channel dies, before any retry | `WithPublisherOptionsOnChannelLost(fn)` |
| Publisher | The publisher is fully rebuilt (exchange + handlers + confirm mode) | `WithPublisherOptionsOnReconnect(fn)` |

Hooks fire **synchronously** and **exactly once** per event. They receive the underlying error that triggered the cycle. If you need non-blocking behavior, spawn a goroutine inside the callback.

```go
consumer, err := rabbitmq.NewConsumer(conn, "my_queue",
    rabbitmq.WithConsumerOptionsOnChannelLost(func(err error) {
        // Fires immediately on disconnect — fast alerting / readiness toggle
        metrics.IncCounter("amqp_disconnect")
        readiness.MarkUnhealthy()
    }),
    rabbitmq.WithConsumerOptionsOnReconnect(func(err error) {
        // Fires after consumer is back on the broker — take a snapshot,
        // replay missed messages from another source, mark healthy
        readiness.MarkHealthy()
        snapshot.SaveCheckpoint()
    }),
)
```

### Configurable backoff

Replace the fixed reconnect interval with a function for exponential backoff with jitter — important at scale to avoid thundering-herd against a recovering broker:

```go
import "math/rand"

conn, err := rabbitmq.NewConn(url,
    rabbitmq.WithConnectionOptionsBackoff(func(attempt int) time.Duration {
        // 2s, 4s, 8s, 16s, 32s, 64s, capped at 60s, with jitter
        base := time.Duration(1<<min(attempt, 6)) * time.Second
        if base > 60*time.Second {
            base = 60 * time.Second
        }
        jitter := time.Duration(rand.Int63n(int64(base / 4)))
        return base + jitter
    }),
)
```

When unset, `ReconnectInterval` is used as a fixed delay (matches the v0.1 default of 5s).

### Bounded retry

By default the library retries reconnection forever. For batch jobs, CLI tools, or any context where indefinite retry is wrong, set a cap:

```go
conn, err := rabbitmq.NewConn(url,
    rabbitmq.WithConnectionOptionsMaxReconnectAttempts(5),
)
```

When the cap is reached, the connection manager closes itself. Active `Consumer.Run` and the publisher recovery loop exit cleanly so your application can shut down or report failure.

### Reconnection count for metrics

Each layer exposes its successful-reconnect counter:

```go
prometheus.NewGauge(...).Set(float64(conn.ReconnectionCount()))
prometheus.NewGauge(...).Set(float64(consumer.ReconnectionCount()))
prometheus.NewGauge(...).Set(float64(publisher.ReconnectionCount()))
```

Useful for stability dashboards and alerting on flap rates.

## Closing and resources

Close your publishers and consumers when you're done with them and do *not* attempt to reuse them. Only close the connection itself once you've closed all associated publishers and consumers.

`Close()` is safe to call multiple times — subsequent calls are no-ops. As of v0.1.2, calling `conn.Close()` without first closing consumers/publishers no longer leaks goroutines; the library propagates closure to all child managers.

## Stability

Note that the API is currently in `v0`. I don't plan on huge changes, but there may be some small breaking changes before we hit `v1`.

## Integration testing

By setting `ENABLE_DOCKER_INTEGRATION_TESTS=TRUE` during `go test -v ./...`, the integration tests will run. These launch a rabbitmq container in the local Docker daemon and test some publish/consume actions.

See [integration_test.go](integration_test.go) for the original pub/sub round-trip test, and [integration_reliability_test.go](integration_reliability_test.go) for regression coverage of the reconnect, ghost-consumer, close-lifecycle, backoff, bounded-retry, and lifecycle-hook scenarios introduced in v0.1.2 and v0.2.0.

## 💬 Contact

[![Twitter Follow](https://img.shields.io/twitter/follow/wagslane.svg?label=Follow%20Wagslane&style=social)](https://twitter.com/intent/follow?screen_name=wagslane)

Submit an issue here on GitHub

## Transient Dependencies

My goal is to keep dependencies limited to 1, [github.com/rabbitmq/amqp091-go](https://github.com/rabbitmq/amqp091-go).

## 👏 Contributing

I would love your help! Contribute by forking the repo and opening pull requests. Please ensure that your code passes the existing tests and linting, and write tests to test your changes if applicable.

All pull requests should be submitted to the `main` branch.
