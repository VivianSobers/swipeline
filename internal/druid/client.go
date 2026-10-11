package druid

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"time"
)

var ErrSpecConflict = errors.New("druid supervisor spec conflict")

type Client struct {
	Base string
	HTTP *http.Client
}

func NewClient(base string) *Client {
	return &Client{
		Base: strings.TrimRight(base, "/"),
		HTTP: &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

func extractSupervisorID(specData []byte) (string, error) {
	var top map[string]any
	if err := json.Unmarshal(specData, &top); err != nil {
		return "", fmt.Errorf("invalid supervisor spec: %w", err)
	}
	if id, ok := top["id"].(string); ok && id != "" {
		return id, nil
	}
	if spec, ok := top["spec"].(map[string]any); ok {
		if id, ok := spec["id"].(string); ok && id != "" {
			return id, nil
		}
		if dataSchema, ok := spec["dataSchema"].(map[string]any); ok {
			if ds, ok := dataSchema["dataSource"].(string); ok && ds != "" {
				return ds, nil
			}
		}
	}
	if dataSchema, ok := top["dataSchema"].(map[string]any); ok {
		if ds, ok := dataSchema["dataSource"].(string); ok && ds != "" {
			return ds, nil
		}
	}
	return "", errors.New("unable to determine supervisor id from spec")
}

func subsetEquals(fileVal, serverVal any) bool {
	switch fv := fileVal.(type) {
	case map[string]any:
		sv, ok := serverVal.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range fv {
			sChild, exists := sv[k]
			if !exists {
				return false
			}
			if !subsetEquals(v, sChild) {
				return false
			}
		}
		return true
	case []any:
		sv, ok := serverVal.([]any)
		if !ok || len(fv) != len(sv) {
			return false
		}
		for i := range fv {
			if !subsetEquals(fv[i], sv[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(fileVal, serverVal)
	}
}

func (c *Client) Install(ctx context.Context, spec []byte) (bool, error) {
	id, err := extractSupervisorID(spec)
	if err != nil {
		return false, err
	}

	getURL := fmt.Sprintf("%s/druid/indexer/v1/supervisor/%s", c.Base, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, getURL, nil)
	if err != nil {
		return false, err
	}

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return false, fmt.Errorf("failed to check existing supervisor: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		postURL := fmt.Sprintf("%s/druid/indexer/v1/supervisor", c.Base)
		postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, postURL, bytes.NewReader(spec))
		if err != nil {
			return false, err
		}
		postReq.Header.Set("Content-Type", "application/json")

		postResp, err := c.httpClient().Do(postReq)
		if err != nil {
			return false, fmt.Errorf("failed to create supervisor: %w", err)
		}
		defer postResp.Body.Close()

		if postResp.StatusCode < 200 || postResp.StatusCode >= 300 {
			body, _ := io.ReadAll(postResp.Body)
			return false, fmt.Errorf("unexpected status creating supervisor: %d: %s", postResp.StatusCode, string(body))
		}
		return true, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("unexpected status checking supervisor: %d: %s", resp.StatusCode, string(body))
	}

	var serverDoc map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&serverDoc); err != nil {
		return false, fmt.Errorf("failed to decode existing supervisor: %w", err)
	}

	var fileDoc map[string]any
	if err := json.Unmarshal(spec, &fileDoc); err != nil {
		return false, err
	}

	fileSpec, _ := fileDoc["spec"].(map[string]any)
	if fileSpec == nil {
		fileSpec = fileDoc
	}

	serverSpec, _ := serverDoc["spec"].(map[string]any)
	if serverSpec == nil {
		serverSpec = serverDoc
	}

	for _, section := range []string{"dataSchema", "ioConfig", "tuningConfig"} {
		fSection := fileSpec[section]
		sSection := serverSpec[section]
		if fSection != nil && !subsetEquals(fSection, sSection) {
			return false, ErrSpecConflict
		}
	}

	return false, nil
}

func (c *Client) State(ctx context.Context, id string) (string, error) {
	statusURL := fmt.Sprintf("%s/druid/indexer/v1/supervisor/%s/status", c.Base, id)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
	if err != nil {
		return "", err
	}

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to query supervisor status: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("supervisor status query failed with code %d: %s", resp.StatusCode, string(body))
	}

	var doc map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return "", fmt.Errorf("failed to parse supervisor status: %w", err)
	}

	if payload, ok := doc["payload"].(map[string]any); ok {
		if state, ok := payload["state"].(string); ok {
			return state, nil
		}
	}
	if state, ok := doc["state"].(string); ok {
		return state, nil
	}

	return "", fmt.Errorf("unable to determine supervisor state from response: %+v", doc)
}

func (c *Client) SQL(ctx context.Context, query string, params ...any) ([]map[string]any, error) {
	sqlURL := fmt.Sprintf("%s/druid/v2/sql", c.Base)

	payload := map[string]any{
		"query": query,
	}

	if len(params) > 0 {
		type paramObj struct {
			Type  string `json:"type"`
			Value any    `json:"value"`
		}
		formatted := make([]paramObj, len(params))
		for i, p := range params {
			pType := "VARCHAR"
			switch p.(type) {
			case int, int8, int16, int32, int64:
				pType = "BIGINT"
			case float32, float64:
				pType = "DOUBLE"
			case bool:
				pType = "BOOLEAN"
			}
			formatted[i] = paramObj{Type: pType, Value: p}
		}
		payload["parameters"] = formatted
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sqlURL, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute druid sql: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("druid sql failed with code %d: %s", resp.StatusCode, string(body))
	}

	var rows []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, fmt.Errorf("failed to parse druid sql response: %w", err)
	}

	return rows, nil
}
