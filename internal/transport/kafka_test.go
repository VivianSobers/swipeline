package transport

import (
	"testing"
	"time"
)

func TestReplyClientOptions(t *testing.T) {
	p, err := Producer("127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.OptValue("ProducerLinger") != time.Duration(0) {
		t.Fatal("linger")
	}
	c, err := Consumer("127.0.0.1:1", "swipeline-gateway-v1", func() {})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.OptValue("FetchMinBytes") != int32(1) || c.OptValue("FetchMaxWait") != 10*time.Millisecond {
		t.Fatal("fetch")
	}
	if c.OptValue("DisableAutoCommit") != true {
		t.Fatal("offsets must be committed only after durable arbitration")
	}
}
