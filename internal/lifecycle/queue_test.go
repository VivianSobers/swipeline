package lifecycle

import (
	"path/filepath"
	"testing"
)

func mockLifecycleEvent(txID string, seq int, kind Kind, parentID string) Event {
	evt, _ := New(
		"run-queue",
		txID,
		1,
		seq,
		string(kind),
		parentID,
		1773300000000,
		1773300000000,
		"2026-10-11",
		"USD",
		5000,
		"merchant-01",
		"bank-0",
		"bank-1",
		"synthetic-reference-v1",
		1,
		"2026-10-11",
	)
	return evt
}

func TestDiskQueueEnqueueAndSurvivesReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "lifecycle_queue")

	q, err := OpenQueue(dir)
	if err != nil {
		t.Fatalf("OpenQueue: %v", err)
	}

	tx1 := "1111111111111111111111111111111111111111111111111111111111111111"
	tx2 := "2222222222222222222222222222222222222222222222222222222222222222"
	parent := "0000000000000000000000000000000000000000000000000000000000000000"

	e1 := mockLifecycleEvent(tx1, 2, KindClear, parent)
	e2 := mockLifecycleEvent(tx1, 3, KindChargeback, e1.EventID)
	e3 := mockLifecycleEvent(tx2, 2, KindReverse, parent)

	batch := []Event{e1, e2, e3}
	if err := q.Enqueue(batch); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if q.Len() != 3 {
		t.Errorf("q.Len() = %d, want 3", q.Len())
	}

	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen queue and verify that records persist in arrival order
	reopened, err := OpenQueue(dir)
	if err != nil {
		t.Fatalf("reopen OpenQueue: %v", err)
	}
	defer reopened.Close()

	if reopened.Len() != 3 {
		t.Fatalf("reopened.Len() = %d, want 3", reopened.Len())
	}

	items := reopened.Items()
	if len(items) != 3 {
		t.Fatalf("reopened items len = %d, want 3", len(items))
	}

	for i, want := range batch {
		if items[i].EventID != want.EventID {
			t.Errorf("item %d ID mismatch: got %s, want %s", i, items[i].EventID, want.EventID)
		}
		if items[i].Kind != want.Kind {
			t.Errorf("item %d Kind mismatch: got %s, want %s", i, items[i].Kind, want.Kind)
		}
		if items[i].LifecycleSeq != want.LifecycleSeq {
			t.Errorf("item %d Seq mismatch: got %d, want %d", i, items[i].LifecycleSeq, want.LifecycleSeq)
		}
	}
}

func TestPollEmptyQueue(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "empty_queue")
	q, err := OpenQueue(dir)
	if err != nil {
		t.Fatalf("OpenQueue: %v", err)
	}
	defer q.Close()

	items, err := q.Poll(10)
	if err != nil {
		t.Fatalf("Poll empty queue: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected 0 items, got %d", len(items))
	}
}

func TestPollPendingUntilAcked(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "unacked_queue")
	q, err := OpenQueue(dir)
	if err != nil {
		t.Fatalf("OpenQueue: %v", err)
	}

	tx1 := "1111111111111111111111111111111111111111111111111111111111111111"
	parent := "0000000000000000000000000000000000000000000000000000000000000000"
	e1 := mockLifecycleEvent(tx1, 2, KindClear, parent)
	e2 := mockLifecycleEvent(tx1, 3, KindChargeback, e1.EventID)
	e3 := mockLifecycleEvent(tx1, 2, KindReverse, parent)

	if err := q.Enqueue([]Event{e1, e2, e3}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	polled, err := q.Poll(2)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(polled) != 2 {
		t.Fatalf("polled len = %d, want 2", len(polled))
	}

	// Close queue without acking
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen queue: all 3 items must be re-read and available for Poll
	reopened, err := OpenQueue(dir)
	if err != nil {
		t.Fatalf("reopen OpenQueue: %v", err)
	}
	defer reopened.Close()

	redelivered, err := reopened.Poll(5)
	if err != nil {
		t.Fatalf("Poll after restart: %v", err)
	}
	if len(redelivered) != 3 {
		t.Fatalf("expected 3 redelivered events, got %d", len(redelivered))
	}
	if redelivered[0].EventID != e1.EventID || redelivered[1].EventID != e2.EventID || redelivered[2].EventID != e3.EventID {
		t.Error("redelivered items do not match original arrival order")
	}
}

func TestPollAndAckAdvancesOffset(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "acked_queue")
	q, err := OpenQueue(dir)
	if err != nil {
		t.Fatalf("OpenQueue: %v", err)
	}

	tx1 := "1111111111111111111111111111111111111111111111111111111111111111"
	parent := "0000000000000000000000000000000000000000000000000000000000000000"
	e1 := mockLifecycleEvent(tx1, 2, KindClear, parent)
	e2 := mockLifecycleEvent(tx1, 3, KindChargeback, e1.EventID)
	e3 := mockLifecycleEvent(tx1, 2, KindReverse, parent)

	if err := q.Enqueue([]Event{e1, e2, e3}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	polled, err := q.Poll(2)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if len(polled) != 2 {
		t.Fatalf("polled len = %d, want 2", len(polled))
	}

	// Ack the 2 polled events
	if err := q.Ack([]string{polled[0].EventID, polled[1].EventID}); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// Polling next item returns the 3rd event
	remaining, err := q.Poll(2)
	if err != nil {
		t.Fatalf("Poll remaining: %v", err)
	}
	if len(remaining) != 1 || remaining[0].EventID != e3.EventID {
		t.Fatalf("expected remaining event e3, got: %+v", remaining)
	}

	// Close and reopen queue: acknowledged items are not re-read
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := OpenQueue(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	// e3 was polled before close but not acked, so it should be available upon restart
	repoll, err := reopened.Poll(5)
	if err != nil {
		t.Fatalf("repoll: %v", err)
	}
	if len(repoll) != 1 || repoll[0].EventID != e3.EventID {
		t.Fatalf("expected 1 unacked event e3, got %+v", repoll)
	}

	// Ack e3
	if err := reopened.Ack([]string{e3.EventID}); err != nil {
		t.Fatalf("Ack e3: %v", err)
	}

	// Empty queue
	done, err := reopened.Poll(5)
	if err != nil {
		t.Fatalf("Poll done: %v", err)
	}
	if len(done) != 0 {
		t.Fatalf("expected 0 events after acking all, got %d", len(done))
	}
}
