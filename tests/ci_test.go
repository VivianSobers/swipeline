package tests

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"testing"
)

type ciWorkflow struct {
	triggers    map[string]bool
	concurrency string
	jobs        map[string]*ciJob
}

type ciJob struct {
	ifCond         string
	timeoutMinutes int
	steps          []string
}

func parseCIWorkflow(content string) *ciWorkflow {
	wf := &ciWorkflow{
		triggers: make(map[string]bool),
		jobs:     make(map[string]*ciJob),
	}

	scanner := bufio.NewScanner(strings.NewReader(content))
	var section string
	var currentJob *ciJob

	for scanner.Scan() {
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}

		// Top-level keys (no indent)
		if !strings.HasPrefix(line, " ") && strings.Contains(line, ":") {
			parts := strings.SplitN(line, ":", 2)
			section = strings.TrimSpace(parts[0])
			if section == "concurrency" {
				val := strings.TrimSpace(parts[1])
				if val != "" {
					wf.concurrency = val
				}
			}
			currentJob = nil
			continue
		}

		if section == "concurrency" && strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "    ") {
			wf.concurrency += " " + trimmed
			continue
		}

		if section == "on" && strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "    ") {
			trigger := strings.TrimSuffix(strings.TrimSpace(line), ":")
			wf.triggers[trigger] = true
			continue
		}

		if section == "jobs" {
			// Job declaration (2 spaces indent)
			if strings.HasPrefix(line, "  ") && !strings.HasPrefix(line, "   ") && strings.HasSuffix(trimmed, ":") {
				jobName := strings.TrimSuffix(trimmed, ":")
				currentJob = &ciJob{}
				wf.jobs[jobName] = currentJob
				continue
			}

			if currentJob != nil {
				if strings.HasPrefix(line, "    if:") {
					currentJob.ifCond = strings.TrimSpace(strings.TrimPrefix(trimmed, "if:"))
				}
				if strings.HasPrefix(line, "    timeout-minutes:") {
					val := strings.TrimSpace(strings.TrimPrefix(trimmed, "timeout-minutes:"))
					if n, err := strconv.Atoi(val); err == nil {
						currentJob.timeoutMinutes = n
					}
				}
				if strings.Contains(line, "run:") {
					currentJob.steps = append(currentJob.steps, trimmed)
				} else if strings.HasPrefix(line, "          ") || strings.HasPrefix(line, "        ") {
					currentJob.steps = append(currentJob.steps, trimmed)
				}
			}
		}
	}
	return wf
}

func TestCIWorkflowPins(t *testing.T) {
	data, err := os.ReadFile("../.github/workflows/ci.yml")
	if err != nil {
		data, err = os.ReadFile(".github/workflows/ci.yml")
		if err != nil {
			t.Fatal(err)
		}
	}

	wf := parseCIWorkflow(string(data))

	// 1. Concurrency configuration
	if !strings.Contains(wf.concurrency, "${{ github.workflow }}-${{ github.ref }}") || !strings.Contains(wf.concurrency, "cancel-in-progress: true") {
		t.Errorf("concurrency missing group or cancel-in-progress: %q", wf.concurrency)
	}

	// 2. Triggers
	for _, want := range []string{"push", "pull_request", "workflow_dispatch"} {
		if !wf.triggers[want] {
			t.Errorf("expected trigger %q in ci.yml", want)
		}
	}

	// 3. Jobs presence
	for _, want := range []string{"check", "superset", "batch", "storage", "authorization"} {
		if _, ok := wf.jobs[want]; !ok {
			t.Errorf("missing job %q in ci.yml", want)
		}
	}

	// 4. Check job has no restriction (runs on push and pull_request)
	if checkJob, ok := wf.jobs["check"]; ok {
		if checkJob.ifCond != "" {
			t.Errorf("check job should not have if condition restricting it, got: %q", checkJob.ifCond)
		}
		if checkJob.timeoutMinutes <= 0 {
			t.Error("check job missing explicit timeout-minutes")
		}

		// Check job runs fast checks: make check, make build, deploy-check (both profiles), demo
		checkSteps := strings.Join(checkJob.steps, "\n")
		for _, req := range []string{"make check", "build", "make deploy-check", "COMPOSE_PROFILES=full make deploy-check", "make demo"} {
			if !strings.Contains(checkSteps, req) {
				t.Errorf("check job missing step %q", req)
			}
		}
		for _, forbidden := range []string{"superset-check", "batch-test"} {
			if strings.Contains(checkSteps, forbidden) {
				t.Errorf("check job must no longer run %q", forbidden)
			}
		}
	}

	// 5. Superset job runs on push and pull_request with make superset-check and explicit timeout
	if supersetJob, ok := wf.jobs["superset"]; ok {
		if supersetJob.ifCond != "" {
			t.Errorf("superset job should not have if condition restricting it, got: %q", supersetJob.ifCond)
		}
		if supersetJob.timeoutMinutes <= 0 {
			t.Errorf("superset job missing explicit timeout-minutes")
		} else if supersetJob.timeoutMinutes != 15 {
			t.Errorf("superset job timeout-minutes = %d, want 15", supersetJob.timeoutMinutes)
		}
		supersetSteps := strings.Join(supersetJob.steps, "\n")
		if !strings.Contains(supersetSteps, "make superset-check") {
			t.Errorf("superset job missing make superset-check")
		}
	}

	// 6. Batch job runs on push and pull_request with make spark-check and make batch-test and explicit timeout
	if batchJob, ok := wf.jobs["batch"]; ok {
		if batchJob.ifCond != "" {
			t.Errorf("batch job should not have if condition restricting it, got: %q", batchJob.ifCond)
		}
		if batchJob.timeoutMinutes <= 0 {
			t.Errorf("batch job missing explicit timeout-minutes")
		} else if batchJob.timeoutMinutes != 20 {
			t.Errorf("batch job timeout-minutes = %d, want 20", batchJob.timeoutMinutes)
		}
		batchSteps := strings.Join(batchJob.steps, "\n")
		for _, req := range []string{"make spark-check", "make batch-test"} {
			if !strings.Contains(batchSteps, req) {
				t.Errorf("batch job missing step %q", req)
			}
		}
	}

	// 7. Storage and authorization jobs run only on pull_request and workflow_dispatch
	for _, onDemandJob := range []string{"storage", "authorization"} {
		if job, ok := wf.jobs[onDemandJob]; ok {
			cond := job.ifCond
			if !strings.Contains(cond, "pull_request") || !strings.Contains(cond, "workflow_dispatch") {
				t.Errorf("job %s must run only on pull_request and workflow_dispatch, got if: %q", onDemandJob, cond)
			}
		}
	}
}
