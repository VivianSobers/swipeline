// Package authorize replays generated traffic through the authorization
// gateway and reconciles every request to its durable final outcome.
package authorize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/VivianSobers/swipeline/internal/auth"
	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/lifecycle"
)

// Sentinel errors returned by Client for lifecycle and admission API responses.
var (
	ErrBadRequest         = errors.New("bad request")
	ErrNotFound           = errors.New("not found")
	ErrConflict           = errors.New("conflict")
	ErrServiceUnavailable = errors.New("service unavailable")
)

// StatusError wraps an HTTP error status, response body, and optional sentinel error.
type StatusError struct {
	StatusCode int
	Body       string
	Err        error
}

func (e *StatusError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("HTTP %d (%s): %s", e.StatusCode, e.Err.Error(), e.Body)
	}
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, e.Body)
}

func (e *StatusError) Unwrap() error {
	return e.Err
}

// Client calls one gateway.
type Client struct {
	Base string
	HTTP *http.Client
	// RetryBudget bounds reconciliation, including HTTP calls; zero defaults to 30 seconds.
	RetryBudget time.Duration
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
// same ID. Resubmit only after a POST 429/503 or a status 404.
// Other statuses and invalid finals are immediate errors.
func (c *Client) Send(ctx context.Context, e contract.Event) (auth.Final, error) {
	decode := func(b []byte) (auth.Final, error) {
		var f auth.Final
		err := json.Unmarshal(b, &f)
		if err == nil && (f.TransactionID != e.TransactionID || f.DecisionID == "" || (f.Decision != "APPROVE" && f.Decision != "DECLINE")) {
			err = fmt.Errorf("invalid final for %s", e.TransactionID)
		}
		return f, err
	}

	budget := c.RetryBudget
	if budget <= 0 {
		budget = 30 * time.Second
	}
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	post := true
	backoff := 10 * time.Millisecond
	for ctx.Err() == nil {
		retryPost := false
		if post {
			status, body, err := c.call(ctx, http.MethodPost, "/v1/authorizations", e.TransactionID, e)
			if err == nil && status == http.StatusOK {
				return decode(body)
			}
			if err == nil && status != http.StatusTooManyRequests && status != http.StatusServiceUnavailable {
				return auth.Final{}, fmt.Errorf("authorization HTTP %d: %s", status, body)
			}
			retryPost = err == nil && (status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable)
		}
		if ctx.Err() != nil {
			break
		}
		status, body, err := c.call(ctx, http.MethodGet, "/v1/authorizations/"+e.TransactionID, "", nil)
		if err == nil && status == http.StatusOK {
			return decode(body)
		}
		if err == nil && status != http.StatusAccepted && status != http.StatusNotFound && status != http.StatusServiceUnavailable {
			return auth.Final{}, fmt.Errorf("status HTTP %d: %s", status, body)
		}
		post = retryPost || (err == nil && status == http.StatusNotFound)
		delay := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			delay.Stop()
		case <-delay.C:
		}
		backoff = min(2*backoff, 500*time.Millisecond)
	}
	if parent.Err() != nil {
		return auth.Final{}, parent.Err()
	}
	return auth.Final{}, fmt.Errorf("unresolved authorization %s: rerun the same run to resume", e.TransactionID)
}

// SendLifecycle sends a lifecycle transition event to the gateway.
// Returns lifecycle.Receipt on HTTP 202 Accepted, or a typed StatusError on failure.
func (c *Client) SendLifecycle(ctx context.Context, e lifecycle.Event) (lifecycle.Receipt, error) {
	status, body, err := c.call(ctx, http.MethodPost, "/v1/lifecycle", e.EventID, e)
	if err != nil {
		return lifecycle.Receipt{}, err
	}
	if status == http.StatusAccepted {
		var receipt lifecycle.Receipt
		if err := json.Unmarshal(body, &receipt); err != nil {
			return lifecycle.Receipt{}, fmt.Errorf("decode lifecycle receipt: %w", err)
		}
		return receipt, nil
	}
	var sentinel error
	switch status {
	case http.StatusBadRequest:
		sentinel = ErrBadRequest
	case http.StatusNotFound:
		sentinel = ErrNotFound
	case http.StatusConflict:
		sentinel = ErrConflict
	case http.StatusServiceUnavailable:
		sentinel = ErrServiceUnavailable
	}
	return lifecycle.Receipt{}, &StatusError{
		StatusCode: status,
		Body:       string(body),
		Err:        sentinel,
	}
}
