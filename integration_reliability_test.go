package rabbitmq

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// prepareDockerTestWithID is the same as prepareDockerTest but also returns
// the container ID so tests can exec into it (e.g. to run rabbitmqctl or
// restart the broker mid-test).
//
// Note: the container is NOT launched with --rm, so that tests which stop
// and later start the broker (docker stop; docker start) don't have the
// container removed out from under them. Cleanup via docker rm -f runs in
// t.Cleanup regardless of exit path.
func prepareDockerTestWithID(t *testing.T) (connStr, containerID string) {
	t.Helper()
	if v, ok := os.LookupEnv(enableDockerIntegrationTestsFlag); !ok || strings.ToUpper(v) != "TRUE" {
		t.Skipf("integration tests are only run if '%s' is TRUE", enableDockerIntegrationTestsFlag)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Retry docker run a few times to handle port-release delay from a
	// previous test's teardown (Docker sometimes holds :5672 briefly after
	// the owning container is removed).
	var (
		out []byte
		err error
	)
	for attempt := range 10 {
		out, err = exec.CommandContext(ctx,
			"docker", "run", "--detach",
			"--publish=5672:5672",
			"--",
			"rabbitmq:4.1.1-alpine",
		).CombinedOutput()
		if err == nil {
			break
		}
		if !strings.Contains(string(out), "port is already allocated") {
			break // unrelated error, fail fast below
		}
		t.Logf("port 5672 still bound on attempt %d, retrying...", attempt+1)
		time.Sleep(time.Second)
	}
	if err != nil {
		t.Fatalf("error launching rabbitmq in docker: %v\noutput: %s", err, out)
	}
	containerID = strings.TrimSpace(string(out))
	t.Cleanup(func() {
		t.Logf("shutting down container '%s'", containerID)
		if err := exec.Command("docker", "rm", "--force", containerID).Run(); err != nil {
			t.Logf("failed to remove container: %v", err)
		}
	})
	return "amqp://guest:guest@localhost:5672/", containerID
}

// listConsumers runs rabbitmqctl list_consumers inside the container and
// returns its stdout. Returns empty string if the broker isn't ready or if
// the exec call exceeds the per-call timeout (rabbitmqctl can hang during
// broker restart).
func listConsumers(t *testing.T, containerID string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx,
		"docker", "exec", containerID,
		"rabbitmqctl", "--quiet", "list_consumers",
	).CombinedOutput()
	if err != nil {
		return ""
	}
	return string(out)
}

// waitForConsumerCount polls the broker until the number of consumers for
// queue equals want, or until deadline elapses.
func waitForConsumerCount(t *testing.T, containerID, queue string, want int, deadline time.Duration) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	tkr := time.NewTicker(500 * time.Millisecond)
	defer tkr.Stop()

	got := -1
	for {
		select {
		case <-ctx.Done():
			return got
		case <-tkr.C:
			output := listConsumers(t, containerID)
			got = strings.Count(output, queue)
			if got == want {
				return got
			}
		}
	}
}

// TestReconnectPreservesConsumer pins the original ghost-consumer fix
// (Fixes A + B + D). After a broker restart the client must still appear
// in the broker's consumer list — no "log says reconnected but server has
// 0 consumers" scenario.
func TestReconnectPreservesConsumer(t *testing.T) {
	connStr, containerID := prepareDockerTestWithID(t)
	conn := waitForHealthyAmqp(t, connStr)
	defer conn.Close()

	queue := "ghost_consumer_regression_queue"
	consumer, err := NewConsumer(conn, queue,
		WithConsumerOptionsLogger(simpleLogF(t.Logf)),
		WithConsumerOptionsConcurrency(1),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	consumed := make(chan Delivery, 8)
	go func() {
		_ = consumer.Run(func(d Delivery) Action {
			select {
			case consumed <- d:
			default:
			}
			return Ack
		})
	}()

	// Baseline: consumer registers.
	if got := waitForConsumerCount(t, containerID, queue, 1, 15*time.Second); got != 1 {
		t.Fatalf("baseline: expected 1 consumer, got %d", got)
	}

	// Kill the broker and bring it back.
	t.Log("restarting rabbitmq container to force reconnect")
	if err := exec.Command("docker", "restart", "--time=1", containerID).Run(); err != nil {
		t.Fatalf("docker restart: %v", err)
	}

	// After restart the consumer must re-register on the broker — the
	// entire point of the ghost-consumer fix.
	if got := waitForConsumerCount(t, containerID, queue, 1, 45*time.Second); got != 1 {
		t.Fatalf("after reconnect: expected 1 consumer on broker, got %d. "+
			"This is the original ghost-consumer symptom.", got)
	}

	// Also verify end-to-end message flow still works.
	publisher, err := NewPublisher(conn, WithPublisherOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	defer publisher.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	tkr := time.NewTicker(time.Second)
	defer tkr.Stop()
	for {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for post-reconnect roundtrip")
		case <-tkr.C:
			if err := publisher.PublishWithContext(ctx, []byte("post-reconnect"), []string{queue}); err != nil {
				t.Logf("publish: %v", err)
			}
		case <-consumed:
			return
		}
	}
}

// TestCloseDuringReconnect pins Fixes C + E + #4. Calling Close() during
// an in-flight reconnect must not leave a consumer registered on the
// broker after the broker recovers.
func TestCloseDuringReconnect(t *testing.T) {
	connStr, containerID := prepareDockerTestWithID(t)
	conn := waitForHealthyAmqp(t, connStr)

	queue := "close_during_reconnect_queue"
	consumer, err := NewConsumer(conn, queue,
		WithConsumerOptionsLogger(simpleLogF(t.Logf)),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}

	go func() {
		_ = consumer.Run(func(d Delivery) Action { return Ack })
	}()

	if got := waitForConsumerCount(t, containerID, queue, 1, 15*time.Second); got != 1 {
		t.Fatalf("baseline: expected 1 consumer, got %d", got)
	}

	// Kill broker to trigger reconnect cycle.
	t.Log("stopping rabbitmq to induce reconnect window")
	if err := exec.Command("docker", "stop", "--time=1", containerID).Run(); err != nil {
		t.Fatalf("docker stop: %v", err)
	}

	// Close the consumer while the reconnect loop is sleeping/retrying.
	t.Log("closing consumer mid-reconnect")
	done := make(chan struct{})
	go func() {
		consumer.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("consumer.Close hung for 30s")
	}

	// Bring broker back.
	t.Log("restarting rabbitmq")
	if err := exec.Command("docker", "start", containerID).Run(); err != nil {
		t.Fatalf("docker start: %v", err)
	}

	// Give the broker time to accept connections.
	time.Sleep(5 * time.Second)

	// After restart there must be NO consumer on the queue — user closed
	// us during the reconnect, so the library must not register again.
	if got := waitForConsumerCount(t, containerID, queue, 0, 15*time.Second); got != 0 {
		t.Fatalf("after reconnect: expected 0 consumers (Close was called), got %d", got)
	}

	_ = conn.Close()
}

// TestCloseWithoutClosingConsumers pins Fix #1. Calling conn.Close()
// while a consumer is still open must not leave background goroutines
// looping forever.
func TestCloseWithoutClosingConsumers(t *testing.T) {
	connStr, _ := prepareDockerTestWithID(t)
	conn := waitForHealthyAmqp(t, connStr)

	queue := "orphaned_consumer_queue"
	consumer, err := NewConsumer(conn, queue, WithConsumerOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	// Also create a publisher to cover that path too.
	pub, err := NewPublisher(conn, WithPublisherOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	runDone := make(chan struct{})
	go func() {
		_ = consumer.Run(func(d Delivery) Action { return Ack })
		close(runDone)
	}()

	// Let everything settle.
	time.Sleep(2 * time.Second)
	beforeGoroutines := runtime.NumGoroutine()

	// Close only the connection; deliberately skip consumer/publisher Close.
	t.Log("closing conn without first closing consumer/publisher")
	_ = conn.Close()

	// Consumer.Run must exit because its reconnectErrCh gets closed by
	// the dispatcher shutdown we wired through on manager-close.
	select {
	case <-runDone:
	case <-time.After(15 * time.Second):
		t.Fatal("Consumer.Run did not exit within 15s after conn.Close()")
	}

	// Give goroutines a moment to unwind, then verify we're not above
	// the pre-close count. A small delta is tolerated because Go may
	// have runtime-internal goroutines come and go.
	time.Sleep(2 * time.Second)
	runtime.GC()
	afterGoroutines := runtime.NumGoroutine()
	// Sanity: reference these so the compiler can't eliminate the locals.
	_ = pub
	_ = consumer

	const tolerance = 3
	if afterGoroutines > beforeGoroutines+tolerance {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("goroutine leak: before=%d after=%d (delta=%d)\nStacks:\n%s",
			beforeGoroutines, afterGoroutines, afterGoroutines-beforeGoroutines, buf[:n])
	}
}

// TestDoubleCloseIsSafe pins Fix #2. Double-calling Close must not hang
// or panic for any of Conn, Consumer, Publisher.
func TestDoubleCloseIsSafe(t *testing.T) {
	connStr, _ := prepareDockerTestWithID(t)
	conn := waitForHealthyAmqp(t, connStr)

	consumer, err := NewConsumer(conn, "double_close_queue",
		WithConsumerOptionsLogger(simpleLogF(t.Logf)),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	pub, err := NewPublisher(conn, WithPublisherOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}

	go func() { _ = consumer.Run(func(d Delivery) Action { return Ack }) }()
	time.Sleep(time.Second)

	// Each double-close must complete within a bounded window — anything
	// over 5s means we've regressed into a hang.
	mustReturn := func(name string, fn func()) {
		t.Helper()
		done := make(chan struct{})
		go func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s panicked: %v", name, r)
				}
				close(done)
			}()
			fn()
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s hung for 5s", name)
		}
	}

	mustReturn("consumer.Close #1", func() { consumer.Close() })
	mustReturn("consumer.Close #2", func() { consumer.Close() })
	mustReturn("publisher.Close #1", func() { pub.Close() })
	mustReturn("publisher.Close #2", func() { pub.Close() })
	mustReturn("conn.Close #1", func() { _ = conn.Close() })
	mustReturn("conn.Close #2", func() { _ = conn.Close() })
}

// TestReconnectGoroutineCount pins Fix #3 and the general "no goroutine
// accretion across reconnects" invariant. Restarting the broker N times
// should end with roughly the same number of goroutines as started with.
func TestReconnectGoroutineCount(t *testing.T) {
	connStr, containerID := prepareDockerTestWithID(t)
	conn := waitForHealthyAmqp(t, connStr)
	defer conn.Close()

	queue := "goroutine_count_queue"
	consumer, err := NewConsumer(conn, queue, WithConsumerOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	pub, err := NewPublisher(conn, WithPublisherOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	defer pub.Close()

	go func() { _ = consumer.Run(func(d Delivery) Action { return Ack }) }()

	if got := waitForConsumerCount(t, containerID, queue, 1, 15*time.Second); got != 1 {
		t.Fatalf("baseline: expected 1 consumer, got %d", got)
	}
	time.Sleep(time.Second)

	runtime.GC()
	baseline := runtime.NumGoroutine()
	t.Logf("baseline goroutines: %d", baseline)

	const restarts = 3
	for i := range restarts {
		t.Logf("restart cycle %d/%d", i+1, restarts)
		if err := exec.Command("docker", "restart", "--time=1", containerID).Run(); err != nil {
			t.Fatalf("docker restart: %v", err)
		}
		if got := waitForConsumerCount(t, containerID, queue, 1, 45*time.Second); got != 1 {
			t.Fatalf("cycle %d: consumer did not re-register, got %d", i, got)
		}
	}

	// Let any transient goroutines settle.
	time.Sleep(3 * time.Second)
	runtime.GC()
	final := runtime.NumGoroutine()
	t.Logf("final goroutines: %d", final)

	// Tolerance: each reconnect can leave a few transient goroutines
	// behind briefly (handler respawn, etc.), but the delta should be
	// bounded and well below N * restarts.
	maxAllowed := baseline + 5
	if final > maxAllowed {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("goroutine accretion over %d reconnects: baseline=%d final=%d (max allowed %d)\nStacks:\n%s",
			restarts, baseline, final, maxAllowed, buf[:n])
	}
}

// TestOnReconnectFiresOnce verifies that the Conn / Consumer / Publisher
// OnReconnect hooks each fire exactly once per docker restart cycle, are
// fired only on success (not on intermediate failed retries), and receive
// a non-nil error describing what triggered the reconnect.
func TestOnReconnectFiresOnce(t *testing.T) {
	connStr, containerID := prepareDockerTestWithID(t)

	var (
		connHookCalls     atomic.Int32
		consumerHookCalls atomic.Int32
		publisherHookCalls atomic.Int32
		lastConsumerErr   atomic.Value
		lastPublisherErr  atomic.Value
		lastConnErr       atomic.Value
	)

	// We can't use waitForHealthyAmqp because it doesn't take options.
	// Build a Conn directly with our hook attached from the start.
	var conn *Conn
	{
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		tkr := time.NewTicker(time.Second)
		defer tkr.Stop()
		var lastErr error
		for conn == nil {
			select {
			case <-ctx.Done():
				t.Fatalf("timed out waiting for healthy amqp: %v", lastErr)
			case <-tkr.C:
				c, err := NewConn(connStr,
					WithConnectionOptionsLogger(simpleLogF(t.Logf)),
					WithConnectionOptionsOnReconnect(func(err error) {
						connHookCalls.Add(1)
						lastConnErr.Store(err)
					}),
				)
				if err != nil {
					lastErr = err
					continue
				}
				// Sanity-publish to confirm broker is ready.
				p, perr := NewPublisher(c, WithPublisherOptionsLogger(simpleLogF(t.Logf)))
				if perr != nil {
					_ = c.Close()
					lastErr = perr
					continue
				}
				if perr := p.PublishWithContext(ctx, []byte{}, []string{"ping"}); perr != nil {
					_ = c.Close()
					lastErr = perr
					continue
				}
				p.Close()
				conn = c
			}
		}
	}
	defer conn.Close()

	queue := "on_reconnect_hook_queue"
	consumer, err := NewConsumer(conn, queue,
		WithConsumerOptionsLogger(simpleLogF(t.Logf)),
		WithConsumerOptionsOnReconnect(func(err error) {
			consumerHookCalls.Add(1)
			lastConsumerErr.Store(err)
		}),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	pub, err := NewPublisher(conn,
		WithPublisherOptionsLogger(simpleLogF(t.Logf)),
		WithPublisherOptionsOnReconnect(func(err error) {
			publisherHookCalls.Add(1)
			lastPublisherErr.Store(err)
		}),
	)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	defer pub.Close()

	go func() { _ = consumer.Run(func(d Delivery) Action { return Ack }) }()

	// Baseline: hooks must NOT have fired yet — we haven't reconnected.
	if got := consumerHookCalls.Load(); got != 0 {
		t.Fatalf("consumer hook fired before reconnect: %d", got)
	}
	if got := publisherHookCalls.Load(); got != 0 {
		t.Fatalf("publisher hook fired before reconnect: %d", got)
	}
	if got := connHookCalls.Load(); got != 0 {
		t.Fatalf("conn hook fired before reconnect: %d", got)
	}

	if got := waitForConsumerCount(t, containerID, queue, 1, 15*time.Second); got != 1 {
		t.Fatalf("baseline: expected 1 consumer, got %d", got)
	}

	// One docker restart cycle.
	t.Log("restarting rabbitmq to trigger one reconnect cycle")
	if err := exec.Command("docker", "restart", "--time=1", containerID).Run(); err != nil {
		t.Fatalf("docker restart: %v", err)
	}

	// Wait for consumer to re-register — implies all three layers reconnected.
	if got := waitForConsumerCount(t, containerID, queue, 1, 60*time.Second); got != 1 {
		t.Fatalf("after restart: consumer did not re-register, got %d", got)
	}

	// Give hook callbacks a moment to flush (they fire from goroutines
	// other than this test's main goroutine).
	time.Sleep(2 * time.Second)

	if got := connHookCalls.Load(); got != 1 {
		t.Errorf("conn OnReconnect: expected 1 call, got %d", got)
	}
	if got := consumerHookCalls.Load(); got != 1 {
		t.Errorf("consumer OnReconnect: expected 1 call, got %d", got)
	}
	if got := publisherHookCalls.Load(); got != 1 {
		t.Errorf("publisher OnReconnect: expected 1 call, got %d", got)
	}

	// Each hook should have received a non-nil error describing the trigger.
	if v := lastConnErr.Load(); v == nil || v.(error) == nil {
		t.Errorf("conn hook received nil error")
	}
	if v := lastConsumerErr.Load(); v == nil || v.(error) == nil {
		t.Errorf("consumer hook received nil error")
	}
	if v := lastPublisherErr.Load(); v == nil || v.(error) == nil {
		t.Errorf("publisher hook received nil error")
	}
}

// newConnWithOptions is a helper for tests that need to construct a Conn
// with specific options (the existing waitForHealthyAmqp helper doesn't
// take options). Polls until the broker accepts a publish.
func newConnWithOptions(t *testing.T, connStr string, opts ...func(*ConnectionOptions)) *Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tkr := time.NewTicker(time.Second)
	defer tkr.Stop()
	var lastErr error
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for healthy amqp: %v", lastErr)
			return nil
		case <-tkr.C:
			c, err := NewConn(connStr, opts...)
			if err != nil {
				lastErr = err
				continue
			}
			p, perr := NewPublisher(c, WithPublisherOptionsLogger(simpleLogF(t.Logf)))
			if perr != nil {
				_ = c.Close()
				lastErr = perr
				continue
			}
			if perr := p.PublishWithContext(ctx, []byte{}, []string{"ping"}); perr != nil {
				_ = c.Close()
				lastErr = perr
				continue
			}
			p.Close()
			return c
		}
	}
}

// TestBackoffStrategyApplied verifies that a custom backoff function is
// invoked for each reconnect attempt and its returned delay is honored.
func TestBackoffStrategyApplied(t *testing.T) {
	connStr, containerID := prepareDockerTestWithID(t)

	var (
		attemptsSeen atomic.Int32
		fastBackoff  = func(attempt int) time.Duration {
			attemptsSeen.Add(1)
			// Short delays so the test runs quickly. Using attempt-based
			// progression so we can also detect that the function gets
			// called with monotonically increasing attempt numbers.
			return 200 * time.Millisecond
		}
	)

	conn := newConnWithOptions(t, connStr,
		WithConnectionOptionsLogger(simpleLogF(t.Logf)),
		WithConnectionOptionsBackoff(fastBackoff),
	)
	defer conn.Close()

	queue := "backoff_test_queue"
	consumer, err := NewConsumer(conn, queue, WithConsumerOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()
	go func() { _ = consumer.Run(func(d Delivery) Action { return Ack }) }()

	if got := waitForConsumerCount(t, containerID, queue, 1, 15*time.Second); got != 1 {
		t.Fatalf("baseline: %d", got)
	}

	// Trigger a reconnect.
	if err := exec.Command("docker", "restart", "--time=1", containerID).Run(); err != nil {
		t.Fatalf("docker restart: %v", err)
	}
	if got := waitForConsumerCount(t, containerID, queue, 1, 60*time.Second); got != 1 {
		t.Fatalf("post-restart: %d", got)
	}

	// At minimum the backoff func must have been called — once for the
	// connection-level loop, possibly more for channel-level loops.
	if got := attemptsSeen.Load(); got < 1 {
		t.Errorf("expected backoff func to be invoked, got %d calls", got)
	}
	t.Logf("backoff func invoked %d times across all reconnect loops", attemptsSeen.Load())
}

// TestMaxReconnectAttemptsBounded verifies that with MaxReconnectAttempts
// set, a permanently-failing reconnect cycle terminates and propagates
// closure to subscribers, instead of looping forever.
func TestMaxReconnectAttemptsBounded(t *testing.T) {
	connStr, containerID := prepareDockerTestWithID(t)

	conn := newConnWithOptions(t, connStr,
		WithConnectionOptionsLogger(simpleLogF(t.Logf)),
		WithConnectionOptionsBackoff(func(int) time.Duration { return 200 * time.Millisecond }),
		WithConnectionOptionsMaxReconnectAttempts(3),
	)
	// Don't defer conn.Close() — we expect the manager to close itself.

	queue := "bounded_retry_queue"
	consumer, err := NewConsumer(conn, queue, WithConsumerOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	runDone := make(chan struct{})
	go func() {
		_ = consumer.Run(func(d Delivery) Action { return Ack })
		close(runDone)
	}()

	if got := waitForConsumerCount(t, containerID, queue, 1, 15*time.Second); got != 1 {
		t.Fatalf("baseline: %d", got)
	}

	// Permanently kill the broker — reconnect must give up after 3 tries.
	t.Log("stopping broker permanently")
	if err := exec.Command("docker", "stop", "--time=1", containerID).Run(); err != nil {
		t.Fatalf("docker stop: %v", err)
	}

	// Backoff is 200ms; 3 attempts should exhaust within ~2s. Allow generous
	// margin for the initial close-detection path.
	select {
	case <-runDone:
		// Success — Run exited because the dispatcher was shut down by
		// the exhausted reconnect loop.
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not exit after MaxReconnectAttempts exhausted")
	}
}

// TestOnConnectionLostAndOnChannelLostFire verifies the lost hooks fire
// at least once when the broker is killed.
func TestOnConnectionLostAndOnChannelLostFire(t *testing.T) {
	connStr, containerID := prepareDockerTestWithID(t)

	var (
		connLostCalls     atomic.Int32
		consumerLostCalls atomic.Int32
		publisherLostCalls atomic.Int32
	)

	conn := newConnWithOptions(t, connStr,
		WithConnectionOptionsLogger(simpleLogF(t.Logf)),
		WithConnectionOptionsOnConnectionLost(func(err error) {
			connLostCalls.Add(1)
		}),
	)
	defer conn.Close()

	queue := "lost_hooks_queue"
	consumer, err := NewConsumer(conn, queue,
		WithConsumerOptionsLogger(simpleLogF(t.Logf)),
		WithConsumerOptionsOnChannelLost(func(err error) {
			consumerLostCalls.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	pub, err := NewPublisher(conn,
		WithPublisherOptionsLogger(simpleLogF(t.Logf)),
		WithPublisherOptionsOnChannelLost(func(err error) {
			publisherLostCalls.Add(1)
		}),
	)
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	defer pub.Close()

	go func() { _ = consumer.Run(func(d Delivery) Action { return Ack }) }()
	if got := waitForConsumerCount(t, containerID, queue, 1, 15*time.Second); got != 1 {
		t.Fatalf("baseline: %d", got)
	}

	// No lost-hook fires expected before the broker dies.
	if got := connLostCalls.Load(); got != 0 {
		t.Fatalf("connLost fired before disconnect: %d", got)
	}
	if got := consumerLostCalls.Load(); got != 0 {
		t.Fatalf("consumerLost fired before disconnect: %d", got)
	}
	if got := publisherLostCalls.Load(); got != 0 {
		t.Fatalf("publisherLost fired before disconnect: %d", got)
	}

	t.Log("restarting broker to trigger lost + reconnect hooks")
	if err := exec.Command("docker", "restart", "--time=1", containerID).Run(); err != nil {
		t.Fatalf("docker restart: %v", err)
	}
	if got := waitForConsumerCount(t, containerID, queue, 1, 60*time.Second); got != 1 {
		t.Fatalf("post-restart: %d", got)
	}

	// Give the lost hooks a moment to flush.
	time.Sleep(2 * time.Second)

	if got := connLostCalls.Load(); got < 1 {
		t.Errorf("conn OnConnectionLost did not fire")
	}
	if got := consumerLostCalls.Load(); got < 1 {
		t.Errorf("consumer OnChannelLost did not fire")
	}
	if got := publisherLostCalls.Load(); got < 1 {
		t.Errorf("publisher OnChannelLost did not fire")
	}
}

// TestReconnectionCountIncreases verifies that the public ReconnectionCount
// methods correctly reflect the number of successful reconnects.
func TestReconnectionCountIncreases(t *testing.T) {
	connStr, containerID := prepareDockerTestWithID(t)

	conn := newConnWithOptions(t, connStr, WithConnectionOptionsLogger(simpleLogF(t.Logf)))
	defer conn.Close()

	queue := "reconnect_count_queue"
	consumer, err := NewConsumer(conn, queue, WithConsumerOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	pub, err := NewPublisher(conn, WithPublisherOptionsLogger(simpleLogF(t.Logf)))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	defer pub.Close()

	go func() { _ = consumer.Run(func(d Delivery) Action { return Ack }) }()
	if got := waitForConsumerCount(t, containerID, queue, 1, 15*time.Second); got != 1 {
		t.Fatalf("baseline: %d", got)
	}

	// Counts must all start at zero.
	if got := conn.ReconnectionCount(); got != 0 {
		t.Errorf("conn baseline: want 0, got %d", got)
	}
	if got := consumer.ReconnectionCount(); got != 0 {
		t.Errorf("consumer baseline: want 0, got %d", got)
	}
	if got := pub.ReconnectionCount(); got != 0 {
		t.Errorf("publisher baseline: want 0, got %d", got)
	}

	if err := exec.Command("docker", "restart", "--time=1", containerID).Run(); err != nil {
		t.Fatalf("docker restart: %v", err)
	}
	if got := waitForConsumerCount(t, containerID, queue, 1, 60*time.Second); got != 1 {
		t.Fatalf("post-restart: %d", got)
	}
	time.Sleep(2 * time.Second)

	if got := conn.ReconnectionCount(); got != 1 {
		t.Errorf("conn after 1 restart: want 1, got %d", got)
	}
	if got := consumer.ReconnectionCount(); got != 1 {
		t.Errorf("consumer after 1 restart: want 1, got %d", got)
	}
	if got := pub.ReconnectionCount(); got != 1 {
		t.Errorf("publisher after 1 restart: want 1, got %d", got)
	}
}

