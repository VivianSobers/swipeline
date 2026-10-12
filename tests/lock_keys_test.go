package tests

import (
	"os"
	"regexp"
	"strconv"
	"testing"
)

func TestAdvisoryLockKeysSeparated(t *testing.T) {
	storePath := "../internal/store/store.go"
	storeData, err := os.ReadFile(storePath)
	if err != nil {
		storePath = "internal/store/store.go"
		storeData, err = os.ReadFile(storePath)
		if err != nil {
			t.Fatalf("failed to read internal/store/store.go: %v", err)
		}
	}

	batchPath := "../batch/db.py"
	batchData, err := os.ReadFile(batchPath)
	if err != nil {
		batchPath = "batch/db.py"
		batchData, err = os.ReadFile(batchPath)
		if err != nil {
			t.Fatalf("failed to read batch/db.py: %v", err)
		}
	}

	// 1. Collect advisory lock keys from the gateway store
	reStore := regexp.MustCompile(`SELECT pg_advisory_xact_lock\((\d+)\)`)
	matches := reStore.FindAllStringSubmatch(string(storeData), -1)
	if len(matches) == 0 {
		t.Fatal("failed to find pg_advisory_xact_lock calls in store.go")
	}

	gatewayKeys := make(map[int]bool)
	for _, m := range matches {
		key, err := strconv.Atoi(m[1])
		if err != nil {
			t.Fatalf("invalid lock key %q: %v", m[1], err)
		}
		gatewayKeys[key] = true
	}

	// The gateway admission key must be 72002
	if !gatewayKeys[72002] {
		t.Fatalf("expected gateway admission lock key 72002 in store.go, found keys: %v", gatewayKeys)
	}

	// 2. Read the Python constant from batch/db.py
	reBatch := regexp.MustCompile(`(?m)^BATCH_WRITER_LOCK_KEY\s*=\s*(\d+)`)
	batchMatch := reBatch.FindStringSubmatch(string(batchData))
	if len(batchMatch) < 2 {
		t.Fatal("failed to find BATCH_WRITER_LOCK_KEY named constant in batch/db.py")
	}

	batchKey, err := strconv.Atoi(batchMatch[1])
	if err != nil {
		t.Fatalf("invalid BATCH_WRITER_LOCK_KEY %q: %v", batchMatch[1], err)
	}

	// 3. Fail while they are equal: batch writer lock must not block gateway admissions
	if batchKey == 72002 {
		t.Fatalf("batch writer lock key (%d) and gateway admission lock key (72002) must not be equal; batch writer must have separate lock key", batchKey)
	}

	if gatewayKeys[batchKey] {
		t.Fatalf("batch writer lock key (%d) collides with gateway store lock key", batchKey)
	}
}
