// Package authorize replays generated traffic through the authorization
// gateway and reconciles every request to its durable final outcome.
package authorize

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
)

// Client calls one gateway.
type Client struct {
	Base string
	HTTP *http.Client
}

func (c *Client) call(ctx context.Context, method, path, key string, v any) (int, []byte, error) {
	var payload io.Reader
	if v != nil {
		data, err := json.Marshal(v)
		if err != nil {
			return 0, nil, err
		}
		payload = bytes.NewReader(data)
	}
	r, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.Base, "/")+path, payload)
	if err != nil {
		return 0, nil, err
	}
	r.Header.Set("Content-Type", "application/json")
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	response, err := c.HTTP.Do(r)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 65536))
	return response.StatusCode, body, err
}

// Register records the run manifest; only a new or identical manifest passes.
func (c *Client) Register(ctx context.Context, r auth.Run) error {
	status, body, err := c.call(ctx, http.MethodPost, "/v1/runs", "", r)
	if err != nil {
		return err
	}
	if status != http.StatusCreated {
		return fmt.Errorf("register HTTP %d: %s", status, body)
	}
	return nil
}

// Send authorizes e and returns its durable final. A busy or uncertain
// answer (429, 503, transport error) is followed by a status query under the
// same ID, then a resubmission with the same ID after a backoff; a request is
// never re-sent under a new identity. Other statuses are errors.
func (c *Client) Send(ctx context.Context, e contract.Event) (auth.Final, error) {
	decode := func(b []byte) (auth.Final, error) {
		var f auth.Final
		err := json.Unmarshal(b, &f)
		if err == nil && (f.TransactionID != e.TransactionID || f.DecisionID == "" || (f.Decision != "APPROVE" && f.Decision != "DECLINE")) {
			err = fmt.Errorf("invalid final for %s", e.TransactionID)
		}
		return f, err
	}
	for attempt := 0; attempt < 6; attempt++ {
		status, body, err := c.call(ctx, http.MethodPost, "/v1/authorizations", e.TransactionID, e)
		if err == nil && status == http.StatusOK {
			return decode(body)
		}
		if err == nil && status != http.StatusTooManyRequests && status != http.StatusServiceUnavailable {
			return auth.Final{}, fmt.Errorf("authorization HTTP %d: %s", status, body)
		}
		status, body, err = c.call(ctx, http.MethodGet, "/v1/authorizations/"+e.TransactionID, "", nil)
		if err == nil && status == http.StatusOK {
			return decode(body)
		}
		if err == nil && status != http.StatusAccepted && status != http.StatusNotFound && status != http.StatusServiceUnavailable {
			return auth.Final{}, fmt.Errorf("status HTTP %d: %s", status, body)
		}
		delay := time.NewTimer(time.Duration(10<<attempt) * time.Millisecond)
		select {
		case <-ctx.Done():
			delay.Stop()
			return auth.Final{}, ctx.Err()
		case <-delay.C:
		}
	}
	return auth.Final{}, fmt.Errorf("unresolved authorization %s: rerun the same run to resume", e.TransactionID)
}
