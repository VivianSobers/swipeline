package tests

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/contract"
	"github.com/VivianSobers/swipeline/internal/generator"
	"github.com/VivianSobers/swipeline/internal/preview"
	"github.com/VivianSobers/swipeline/internal/replay"
)

func TestInstalledPreviewAndPrometheus(t *testing.T) {
	base := os.Getenv("SWIPELINE_TEST_URL")
	prom := os.Getenv("SWIPELINE_TEST_PROM_URL")
	if base == "" || prom == "" {
		t.Skip("run make demo")
	}
	client := &http.Client{Timeout: 90 * time.Second}
	get := func(path string) []byte {
		t.Helper()
		response, err := client.Get(base + path)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			t.Fatalf("GET %s: %s", path, response.Status)
		}
		data, err := io.ReadAll(io.LimitReader(response.Body, 32<<20))
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	var before preview.Status
	if err := json.Unmarshal(get("/v1/status"), &before); err != nil {
		t.Fatal(err)
	}
	if before.State != "idle" {
		t.Fatal("test project is not fresh")
	}
	const count = 6000
	requests := []preview.Request{
		{Config: replay.Config{Generator: generator.Config{RunID: "e2e", Seed: 42, Day: "2026-10-08", Events: count}, TPS: 1000, Workers: 1, Queue: 128}, Output: "first"},
		{Config: replay.Config{Generator: generator.Config{RunID: "e2e", Seed: 42, Day: "2026-10-08", Events: count}, TPS: 1000, Workers: 8, Queue: 128}, Output: "second"},
	}
	for _, req := range requests {
		data, _ := json.Marshal(req)
		response, err := client.Post(base+"/v1/replays", "application/json", bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		var status preview.Status
		decodeErr := json.NewDecoder(response.Body).Decode(&status)
		_ = response.Body.Close()
		if decodeErr != nil || response.StatusCode != 200 || status.State != "complete" || status.Progress.Completed != count || status.Progress.PeakInFlight > 128 {
			t.Fatalf("preview failed: %v %s %+v", decodeErr, response.Status, status)
		}
		var manifest struct {
			Mode                   string  `json:"mode"`
			ClockCompressionTarget float64 `json:"clock_compression_target"`
		}
		if err = json.Unmarshal(get("/v1/previews/"+req.Output+"/manifest"), &manifest); err != nil {
			t.Fatal(err)
		}
		if manifest.Mode != "preview-only" || manifest.ClockCompressionTarget != 14400 {
			t.Fatalf("bad manifest %+v", manifest)
		}
	}
	read := func(name string) []contract.Event {
		t.Helper()
		scanner := bufio.NewScanner(bytes.NewReader(get("/v1/previews/" + name)))
		scanner.Buffer(make([]byte, 4096), 1<<20)
		events := make([]contract.Event, 0, count)
		for scanner.Scan() {
			e, err := contract.Decode(scanner.Bytes())
			if err != nil {
				t.Fatal(err)
			}
			events = append(events, e)
		}
		if err := scanner.Err(); err != nil {
			t.Fatal(err)
		}
		if len(events) != count {
			t.Fatal("wrong row count")
		}
		return events
	}
	first, second := read("first"), read("second")
	for i, a := range first {
		b := second[i]
		if a.SourceSeq != int64(i) || a.TransactionID != b.TransactionID || a.EventID != b.EventID || a.PayloadHash != b.PayloadHash || !bytes.Equal(contract.Canonical(a.Business), contract.Canonical(b.Business)) {
			t.Fatalf("replay/concurrency mismatch at %d", i)
		}
	}
	query := func(expression string) (float64, error) {
		response, err := client.Get(prom + "/api/v1/query?query=" + url.QueryEscape(expression))
		if err != nil {
			return 0, err
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			return 0, fmt.Errorf("Prometheus: %s", response.Status)
		}
		var answer struct {
			Status string `json:"status"`
			Data   struct {
				Result []struct {
					Value []json.RawMessage `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		if err = json.NewDecoder(response.Body).Decode(&answer); err != nil {
			return 0, err
		}
		if answer.Status != "success" || len(answer.Data.Result) != 1 || len(answer.Data.Result[0].Value) != 2 {
			return 0, fmt.Errorf("series unavailable")
		}
		var text string
		if err = json.Unmarshal(answer.Data.Result[0].Value[1], &text); err != nil {
			return 0, err
		}
		return strconv.ParseFloat(text, 64)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		total, err := query(`swipeline_replayer_completed_total{job="replayer"}`)
		up, upErr := query(`up{job="replayer"}`)
		if err == nil && upErr == nil && total == 2*count && up == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("scrape not visible: total=%v err=%v up=%v err=%v", total, err, up, upErr)
		}
		time.Sleep(time.Second)
	}
	if strings.Contains(string(get("/metrics")), "run_id=") {
		t.Fatal("identifier labels leaked")
	}
	t.Log("two ordered 6000-event previews match; Prometheus reports 12000 completed preview records")
}
