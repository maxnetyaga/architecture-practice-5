//go:build integration
// +build integration

package integration

import (
	"net/http"
	"sync"
	"testing"
	"time"

)

func waitForBalancer(t *testing.T, url string, timeout time.Duration) {
    deadline := time.Now().Add(timeout)
    for time.Now().Before(deadline) {
        resp, err := http.Get(url)
        if err == nil {
            resp.Body.Close()
            return
        }
        time.Sleep(200 * time.Millisecond)
    }
    t.Fatalf("Balancer at %s did not accept connections within %v", url, timeout)
}

func TestBalancerDistribution(t *testing.T) {
	const requests = 30
	url := "http://balancer:8090"

	seen := make(map[string]struct{})
	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(requests)

	for i := 0; i < requests; i++ {
		go func(reqNum int) {
			defer wg.Done()
			defer mu.Unlock()

			resp, err := http.Get(url)
			if err != nil {
				t.Errorf("Failed to send request #%d: %v", reqNum+1, err)
				return
			}
			defer resp.Body.Close()

			from := resp.Header.Get("lb-from")
			if from == "" {
				t.Errorf("Response #%d missing 'lb-from' header", reqNum+1)
				return
			}

			mu.Lock()
			seen[from] = struct{}{}
		}(i)
	}

	wg.Wait()

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
