package authorize

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/VivianSobers/swipeline/internal/auth"
)

func TestRegisterAcceptsOnlyCreated(t *testing.T) {
	status := http.StatusCreated
	var got auth.Run
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/runs" {
			t.Errorf("%s %s", r.Method, r.URL.Path)
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(status)
	}))
	defer s.Close()
	c := Client{Base: s.URL + "/", HTTP: s.Client()}
	if err := c.Register(context.Background(), auth.Run{ID: "run"}); err != nil || got.ID != "run" {
		t.Fatal(err, got)
	}
	status = http.StatusConflict
	if c.Register(context.Background(), auth.Run{}) == nil {
		t.Fatal("conflict ignored")
	}
	s.Close()
	if c.Register(context.Background(), auth.Run{}) == nil {
		t.Fatal("unreachable gateway ignored")
	}
}
