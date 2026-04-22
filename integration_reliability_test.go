package rabbitmq

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"strings"
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

