package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/VivianSobers/swipeline/internal/authorize"
)

func TestInstalledSparseLatency(t *testing.T) {
	t.Logf("budgets enforced=%t cpus=%d", enforceBudgets(), runtime.NumCPU())
	c := installedClient(t)
	flink := os.Getenv("SWIPELINE_TEST_FLINK")
	if flink == "" {
		t.Skip("run make m2-latency")
	}
	expected := os.Getenv("SWIPELINE_EXPECT_BUFFER_MS")
	if expected != "0" && expected != "100" {
		t.Fatal("expected buffer must be 0 or 100 ms")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	get := func(url string) []byte {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		res, err := c.HTTP.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		data, err := io.ReadAll(res.Body)
		if err != nil || res.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: HTTP %d %v", url, res.StatusCode, err)
		}
		return data
	}
	var overview struct {
		Jobs []struct {
			ID    string `json:"jid"`
			State string `json:"state"`
		} `json:"jobs"`
	}
	if err := json.Unmarshal(get(flink+"/jobs/overview"), &overview); err != nil {
		t.Fatal(err)
	}
	if len(overview.Jobs) != 1 || overview.Jobs[0].State != "RUNNING" {
		t.Fatalf("expected one running job: %+v", overview)
	}
	var config struct {
		Execution struct {
			User map[string]string `json:"user-config"`
		} `json:"execution-config"`
	}
	if err := json.Unmarshal(get(flink+"/jobs/"+overview.Jobs[0].ID+"/config"), &config); err != nil {
		t.Fatal(err)
	}
	effective := config.Execution.User["authorization.buffer-timeout-ms"]
	if effective != expected {
		t.Fatalf("effective running-job buffer timeout=%q, want %s ms", effective, expected)
	}
	t.Logf("running-job buffer timeout=%s ms", effective)
	suffix := expected + "-" + fmt.Sprint(time.Now().UnixNano())
	if _, err := authorize.Run(ctx, installedConfig("sparse-warm-"+suffix, 300, 50), c, "latency", 128); err != nil {
		t.Fatal(err)
	}
	before := string(get(c.Base + "/metrics"))
	summary, err := authorize.Run(ctx, installedConfig("sparse-"+suffix, 1000, 20), c, "latency", 128)
	if err != nil || summary.Completed != 1000 {
		t.Fatalf("sparse replay: %+v %v", summary, err)
	}
	verdict, err := authorize.Gate(before, string(get(c.Base+"/metrics")), "latency", 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("buffer=%s ms pass=%t p99=%s stand-in-share=%.3f%% samples=%d completed=%d", effective, verdict.Pass, verdict.P99, 100*verdict.StandInShare, verdict.Samples, summary.Completed)
	if enforceBudgets() && verdict.Pass != (expected == "0") {
		t.Fatalf("unexpected latency gate verdict: %+v", verdict)
	}
}
