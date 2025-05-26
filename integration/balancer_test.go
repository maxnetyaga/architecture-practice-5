package integration

import (
	"net/http"
	"testing"
)

func TestBalancerDistribution(t *testing.T) {
	const requests = 10
	url := "http://lb:8080"

	seen := make(map[string]struct{})

	for i := 0; i < requests; i++ {
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}
		resp.Body.Close()

		from := resp.Header.Get("lb-from")
		if from == "" {
			t.Fatalf("missing lb-from header")
		}
		seen[from] = struct{}{}
	}

	if len(seen) < 2 {
		t.Errorf("expected distribution to at least 2 servers, got %d", len(seen))
	}
}
