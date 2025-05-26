//go:build integration
// +build integration

package integration

import (
	"net/http"
	"os"
	"testing"
	"time"

)

func waitForBalancer(t *testing.T, url string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url + "/")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("Balancer at %s did not respond 200 OK within %v", url, timeout)
}

func TestBalancerDistribution(t *testing.T) {
	const requests = 10
	host := os.Getenv("BALANCER_HOST")
	if host == "" {
		host = "balancer:8080"
	}
	url := "http://" + host

	waitForBalancer(t, url, 10*time.Second)

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
	} else {
		t.Logf("Requests were distributed across %d servers: %v", len(seen), keys(seen))
	}
}

func keys(m map[string]struct{}) []string {
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	return result
}
