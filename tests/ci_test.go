package tests

import (
	"bufio"
	"os"
	"strings"
	"testing"
)

type ciWorkflow struct {
	triggers    map[string]bool
	concurrency string
	jobs        map[string]*ciJob
}

type ciJob struct {
	ifCond string
	steps  []string
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
	for _, want := range []string{"check", "storage", "authorization"} {
		if _, ok := wf.jobs[want]; !ok {
			t.Fatalf("missing job %q in ci.yml", want)
		}
	}

	// 4. Check job has no restriction (runs on push and pull_request)
	if wf.jobs["check"].ifCond != "" {
		t.Errorf("check job should not have if condition restricting it, got: %q", wf.jobs["check"].ifCond)
	}

	// Check job runs fast checks: make check, deploy-check (both profiles), batch-test
	checkSteps := strings.Join(wf.jobs["check"].steps, "\n")
	for _, req := range []string{"make check", "make batch-test", "make deploy-check", "COMPOSE_PROFILES=full make deploy-check"} {
		if !strings.Contains(checkSteps, req) {
			t.Errorf("check job missing step %q", req)
		}
	}

	// 5. Storage and authorization jobs run only on pull_request and workflow_dispatch
	for _, onDemandJob := range []string{"storage", "authorization"} {
		cond := wf.jobs[onDemandJob].ifCond
		if !strings.Contains(cond, "pull_request") || !strings.Contains(cond, "workflow_dispatch") {
			t.Errorf("job %s must run only on pull_request and workflow_dispatch, got if: %q", onDemandJob, cond)
		}
	}
}
