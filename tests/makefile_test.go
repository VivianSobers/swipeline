package tests

import (
	"os"
	"regexp"
	"testing"
)

// Every bind mount the Makefile passes to docker run must carry the shared
// SELinux label so checks work on hosts where Docker enforces SELinux.
func TestMakefileBindMountsAreSELinuxShared(t *testing.T) {
	data, err := os.ReadFile("../Makefile")
	if err != nil {
		t.Fatal(err)
	}
	mounts := regexp.MustCompile(`-v "([^"]+)"`).FindAllStringSubmatch(string(data), -1)
	if len(mounts) == 0 {
		t.Fatal("no bind mounts found")
	}
	labelled := regexp.MustCompile(`:(ro,)?z$`)
	for _, m := range mounts {
		if !labelled.MatchString(m[1]) {
			t.Errorf("bind mount without z label: %s", m[1])
		}
	}
}
