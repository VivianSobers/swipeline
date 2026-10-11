package lifecycle

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

// DiskQueue represents a persistent, disk-backed FIFO queue for scheduled lifecycle events.
type DiskQueue struct {
	dir             string
	file            *os.File
	mu              sync.Mutex
	items           []Event
	readPos         int
	committedOffset int64
	acked           map[string]bool
}

// OpenQueue opens or initializes a disk-backed queue in the specified directory.
func OpenQueue(dir string) (*DiskQueue, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	offsetPath := filepath.Join(dir, "committed.offset")
	var committedOffset int64
	if data, err := os.ReadFile(offsetPath); err == nil {
		if val, err := strconv.ParseInt(string(bytes.TrimSpace(data)), 10, 64); err == nil {
			committedOffset = val
		}
	}

	walPath := filepath.Join(dir, "events.wal")
	var items []Event

	if data, err := os.ReadFile(walPath); err == nil && len(data) > 0 {
		scanner := bufio.NewScanner(bytes.NewReader(data))
		buf := make([]byte, 64*1024)
		scanner.Buffer(buf, 1024*1024)
		var lineIdx int64
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			if lineIdx >= committedOffset {
				var e Event
				if err := json.Unmarshal(line, &e); err == nil {
					items = append(items, e)
				}
			}
			lineIdx++
		}
	}

	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &DiskQueue{
		dir:             dir,
		file:            f,
		items:           items,
		readPos:         0,
		committedOffset: committedOffset,
		acked:           make(map[string]bool),
	}, nil
}

// Enqueue persists scheduled lifecycle events to the write-ahead log.
func (q *DiskQueue) Enqueue(events []Event) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, e := range events {
		data, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if _, err := q.file.Write(append(data, '\n')); err != nil {
			return err
		}
		q.items = append(q.items, e)
	}

	return q.file.Sync()
}

// Poll retrieves up to n unpolled, unacknowledged events from the queue in FIFO order.
func (q *DiskQueue) Poll(n int) ([]Event, error) {
	if n <= 0 {
		return []Event{}, nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	var result []Event
	for q.readPos < len(q.items) && len(result) < n {
		item := q.items[q.readPos]
		q.readPos++
		if !q.acked[item.EventID] {
			result = append(result, item)
		}
	}
	if result == nil {
		result = []Event{}
	}
	return result, nil
}

// Ack marks the specified events as acknowledged and advances the persistent head offset.
func (q *DiskQueue) Ack(eventIDs []string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, id := range eventIDs {
		q.acked[id] = true
	}

	advanced := false
	for len(q.items) > 0 && q.acked[q.items[0].EventID] {
		delete(q.acked, q.items[0].EventID)
		q.items = q.items[1:]
		q.committedOffset++
		if q.readPos > 0 {
			q.readPos--
		}
		advanced = true
	}

	if advanced {
		return q.saveCommittedOffset()
	}
	return nil
}

func (q *DiskQueue) saveCommittedOffset() error {
	tmpPath := filepath.Join(q.dir, "committed.offset.tmp")
	finalPath := filepath.Join(q.dir, "committed.offset")
	data := []byte(strconv.FormatInt(q.committedOffset, 10) + "\n")
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, finalPath)
}

// Len returns the count of uncommitted items in the queue.
func (q *DiskQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

// Items returns a copy of all uncommitted items in the queue.
func (q *DiskQueue) Items() []Event {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]Event, len(q.items))
	copy(out, q.items)
	return out
}

// Close flushes and closes the queue write-ahead log.
func (q *DiskQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.file != nil {
		_ = q.file.Sync()
		err := q.file.Close()
		q.file = nil
		return err
	}
	return nil
}
