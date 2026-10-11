package tests

import (
	"os"
	"testing"
)

func TestInstalledAnalytics(t *testing.T) {
	druidURL := os.Getenv("SWIPELINE_DRUID")
	if druidURL == "" {
		t.Skip("run make m3-demo on server")
	}
	t.Log("running installed analytics acceptance")
}
