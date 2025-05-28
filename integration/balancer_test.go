//go:build integration
// +build integration

package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

const requests = 30

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
	teamName := os.Getenv("TEAM_NAME")
	if teamName == "" {
		t.Fatal("Environment variable TEAM_NAME is required")
	}

	url := fmt.Sprintf("http://balancer:8090/api/v1/some-data?key=%s", teamName)

	waitForBalancer(t, url, 10*time.Second)

	seen := make(map[string]struct{})
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(requests)

	for i := 0; i < requests; i++ {
		go func(reqNum int) {
			defer wg.Done()

			resp, err := http.Get(url)
			if err != nil {
				t.Errorf("Request #%d failed: %v", reqNum+1, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Errorf("Request #%d unexpected status: %d", reqNum+1, resp.StatusCode)
				return
			}

			var body struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				t.Errorf("Request #%d invalid JSON: %v", reqNum+1, err)
				return
			}
			if body.Key != teamName {
				t.Errorf("Request #%d unexpected key: %q", reqNum+1, body.Key)
				return
			}
			if body.Value == "" {
				t.Errorf("Request #%d empty value", reqNum+1)
				return
			}

			from := resp.Header.Get("lb-from")
			if from == "" {
				t.Errorf("Request #%d missing 'lb-from' header", reqNum+1)
				return
			}

			mu.Lock()
			seen[from] = struct{}{}
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	servers := make([]string, 0, len(seen))
	for s := range seen {
		servers = append(servers, s)
	}
	if len(servers) < 2 {
		t.Errorf("Expected at least 2 different servers, got %d: %v", len(servers), servers)
	} else {
		t.Logf("Distributed across %d servers: %v", len(servers), servers)
	}
}
