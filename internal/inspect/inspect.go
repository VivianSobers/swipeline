// Package inspect provides transaction lifecycle inspection from Hudi lakehouse tables.
package inspect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
)

// Result represents the lifecycle state and history of a transaction.
type Result struct {
	TransactionID string         `json:"transaction_id"`
	Status        string         `json:"status"`
	Amounts       Amounts        `json:"amounts"`
	StateHash     string         `json:"state_hash"`
	StateVersion  int            `json:"state_version"`
	Events        []JournalEvent `json:"events"`
	Postings      []Posting      `json:"postings"`
}

// Amounts contains the financial amounts for a transaction.
type Amounts struct {
	AuthorizedAmount  int64  `json:"authorized_amount"`
	ClearedAmount     int64  `json:"cleared_amount"`
	ReversedAmount    int64  `json:"reversed_amount"`
	ChargedBackAmount int64  `json:"charged_back_amount"`
	Currency          string `json:"currency"`
}

// JournalEvent contains an audit entry for a transaction lifecycle transition.
type JournalEvent struct {
	EventID     string `json:"event_id"`
	LogicalSeq  int    `json:"logical_seq"`
	EventType   string `json:"event_type"`
	EventTimeMS int64  `json:"event_time_ms"`
	PayloadHash string `json:"payload_hash"`
}

// Posting contains an accounting financial posting generated for settlement.
type Posting struct {
	PostingID          string  `json:"posting_id"`
	EventID            string  `json:"event_id"`
	PostingDate        string  `json:"posting_date"`
	SignedPrincipal    int64   `json:"signed_principal"`
	SignedInterchange  int64   `json:"signed_interchange"`
	FeeScheduleVersion string  `json:"fee_schedule_version"`
	AdjustmentReason   *string `json:"adjustment_reason"`
	BatchID            string  `json:"batch_id"`
}

// Inspect queries transaction lifecycle status and history.
func Inspect(ctx context.Context, txID, sparkURL string) (Result, error) {
	if txID == "" {
		return Result{}, errors.New("transaction ID is required")
	}

	// 1. If sparkURL is an HTTP endpoint, query it via HTTP GET
	if strings.HasPrefix(sparkURL, "http://") || strings.HasPrefix(sparkURL, "https://") {
		u := strings.TrimRight(sparkURL, "/") + "/inspect?transaction_id=" + url.QueryEscape(txID)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return Result{}, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return Result{}, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return Result{}, fmt.Errorf("inspect HTTP %d: %s", resp.StatusCode, string(body))
		}
		var res Result
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			return Result{}, err
		}
		return res, nil
	}

	// 2. Otherwise invoke batch inspect CLI via exec
	var cmd *exec.Cmd
	if sparkURL != "" {
		parts := strings.Fields(sparkURL)
		args := append(parts[1:], "--transaction-id", txID)
		cmd = exec.CommandContext(ctx, parts[0], args...)
	} else if os.Getenv("SWIPELINE_INSPECT_CMD") != "" {
		parts := strings.Fields(os.Getenv("SWIPELINE_INSPECT_CMD"))
		args := append(parts[1:], "--transaction-id", txID)
		cmd = exec.CommandContext(ctx, parts[0], args...)
	} else {
		cmd = exec.CommandContext(ctx, "docker", "compose", "exec", "-T", "spark-batch", "python3", "-m", "batch.inspect", "--transaction-id", txID)
	}

	output, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return Result{}, fmt.Errorf("inspect command failed: %s: %s", exitErr.Error(), string(exitErr.Stderr))
		}
		return Result{}, fmt.Errorf("failed to run inspect: %w", err)
	}

	var res Result
	if err := json.Unmarshal(output, &res); err != nil {
		return Result{}, fmt.Errorf("failed to parse inspect JSON output: %w (raw output: %s)", err, string(output))
	}
	return res, nil
}
