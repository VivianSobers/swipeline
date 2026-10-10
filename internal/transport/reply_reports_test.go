package transport

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/gateway"
)

func TestReplyReportsThrottlePerMessage(t *testing.T) {
	var output bytes.Buffer
	s := &gateway.Service{}
	reports := replyReports{service: s, out: &output, last: map[string]time.Time{}}
	start := time.Unix(100, 0)
	first, second := errors.New("first failure"), errors.New("second failure")
	reports.retry(first, start)
	reports.retry(first, start.Add(time.Second-time.Nanosecond))
	reports.retry(second, start.Add(time.Second-time.Nanosecond))
	reports.retry(first, start.Add(time.Second))
	if got := output.String(); strings.Count(got, first.Error()) != 2 || strings.Count(got, second.Error()) != 1 {
		t.Fatalf("per-message one-second limit: %s", got)
	}
	if got := s.ReplyRetries.Load(); got != 4 {
		t.Fatalf("retries=%d, want 4", got)
	}
}

func TestReplyReportsConcurrentRetries(t *testing.T) {
	var output bytes.Buffer
	s := &gateway.Service{}
	reports := replyReports{service: s, out: &output, last: map[string]time.Time{}}
	var wg sync.WaitGroup
	for i := 0; i < ReplyPollRecords; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); reports.retry(errors.New("shared failure"), time.Unix(100, 0)) }()
	}
	wg.Wait()
	if got := strings.Count(output.String(), "shared failure"); got != 1 {
		t.Fatalf("reports=%d, want 1", got)
	}
	if got := s.ReplyRetries.Load(); got != ReplyPollRecords {
		t.Fatalf("retries=%d, want %d", got, ReplyPollRecords)
	}
}
