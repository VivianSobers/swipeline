package transport

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/VivianSobers/swipeline/internal/gateway"
)

// replyReports shares the stderr throttle across concurrent records and counts
// every retry, including reports suppressed by the one-second message limit.
type replyReports struct {
	mu      sync.Mutex
	service *gateway.Service
	out     io.Writer
	last    map[string]time.Time
}

func (r *replyReports) retry(err error, now time.Time) {
	r.service.ReplyRetries.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	// Retain only recent messages, so historical errors do not accumulate.
	for message, at := range r.last {
		if now.Sub(at) >= time.Second {
			delete(r.last, message)
		}
	}
	message := err.Error()
	if at, exists := r.last[message]; exists && now.Sub(at) < time.Second {
		return
	}
	fmt.Fprintf(r.out, "reply: %v\n", err)
	r.last[message] = now
}
