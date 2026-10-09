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

	"github.com/VivianSobers/swipeline/internal/auth"
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
