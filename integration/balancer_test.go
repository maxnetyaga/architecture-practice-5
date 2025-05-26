//go:build integration
// +build integration

package integration

import (
	"net/http"
	"testing"
)

func TestBalancerDistribution(t *testing.T) {
	const requests = 10
	url := "http://balancer:8080"

	seen := make(map[string]struct{})

	for i := 0; i < requests; i++ {
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("Failed to send request #%d: %v", i+1, err)
		}
		resp.Body.Close()

		from := resp.Header.Get("lb-from")
		if from == "" {
			t.Fatalf("Response #%d missing 'lb-from' header", i+1)
		}
		seen[from] = struct{}{}
	}

	if len(seen) < 2 {
		t.Errorf("Expected requests to be distributed to at least 2 servers, but got only %d. Servers seen: %v", len(seen), keys(seen))
	}
}

func keys(m map[string]struct{}) []string {
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	return result
}