package dispatcher

import (
	"log"
	"math"
	"math/rand"
	"sync"
	"time"
)

// Dispatcher -
type Dispatcher struct {
	subscribers   map[int]dispatchSubscriber
	subscribersMu *sync.Mutex

	// shutdownCh is closed by Shutdown() to force-close all subscribers —
	// used when the owning manager shuts down without individual
	// unsubscribes (e.g. user closed the connection without closing every
	// consumer first).
	shutdownCh   chan struct{}
	shutdownOnce sync.Once
}

type dispatchSubscriber struct {
	notifyCancelOrCloseChan chan error
	closeCh                 <-chan struct{}
}

// NewDispatcher -
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		subscribers:   make(map[int]dispatchSubscriber),
		subscribersMu: &sync.Mutex{},
		shutdownCh:    make(chan struct{}),
	}
}

// Shutdown forcibly closes every subscriber's notify channel so owners
// blocked on `for err := range ch` can exit. Idempotent.
func (d *Dispatcher) Shutdown() {
	d.shutdownOnce.Do(func() {
		close(d.shutdownCh)
	})
}

// Dispatch -
func (d *Dispatcher) Dispatch(err error) error {
	d.subscribersMu.Lock()
	defer d.subscribersMu.Unlock()
	for _, subscriber := range d.subscribers {
		select {
		case <-time.After(time.Second * 5):
			log.Println("Unexpected rabbitmq error: timeout in dispatch")
		case subscriber.notifyCancelOrCloseChan <- err:
		}
	}
	return nil
}

// AddSubscriber -
func (d *Dispatcher) AddSubscriber() (<-chan error, chan<- struct{}) {
	const maxRand = math.MaxInt
	const minRand = 0
	id := rand.Intn(maxRand-minRand) + minRand

	// Buffer closeCh so that a send from the subscriber side (e.g. the
	// goroutine in cleanupResources) doesn't block even if we've already
	// closed this subscriber via Shutdown().
	closeCh := make(chan struct{}, 1)
	notifyCancelOrCloseChan := make(chan error)

	d.subscribersMu.Lock()
	d.subscribers[id] = dispatchSubscriber{
		notifyCancelOrCloseChan: notifyCancelOrCloseChan,
		closeCh:                 closeCh,
	}
	d.subscribersMu.Unlock()

	go func(id int) {
		// Exit when either the subscriber unsubscribes or the dispatcher
		// is shut down by its owner — whichever comes first.
		select {
		case <-closeCh:
		case <-d.shutdownCh:
		}
		d.subscribersMu.Lock()
		defer d.subscribersMu.Unlock()
		sub, ok := d.subscribers[id]
		if !ok {
			return
		}
		close(sub.notifyCancelOrCloseChan)
		delete(d.subscribers, id)
	}(id)
	return notifyCancelOrCloseChan, closeCh
}
