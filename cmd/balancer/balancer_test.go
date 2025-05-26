package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// Helper: Create a mock backend server
func newMockServer(t *testing.T, status int, body string) *httptest.Server {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(status)
		w.Write([]byte(body))
	})
	return httptest.NewServer(handler)
}

// Test: Health check returns true for 200 OK
func TestHealthCheckHealthy(t *testing.T) {
	srv := newMockServer(t, http.StatusOK, "OK")
	defer srv.Close()

	addr := srv.Listener.Addr().String()
	*https = false
	timeout = 2 * time.Second

	ok := health(addr)
	if !ok {
		t.Errorf("Expected health check to pass for mock server at %s", addr)
	}
}

// Test: Health check returns false for unreachable or non-200
func TestHealthCheckUnhealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	addr := srv.Listener.Addr().String()
	timeout = 1 * time.Second

	if health(addr) {
		t.Errorf("Expected health check to fail for mock server with 500")
	}
}

// Test: Forwarding works correctly and response body is proxied
func TestForwarding(t *testing.T) {
	const responseText = "Hello from backend"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(responseText))
	}))
	defer srv.Close()

	req := httptest.NewRequest("GET", "http://localhost/", nil)
	w := httptest.NewRecorder()

	addr := srv.Listener.Addr().String()
	err := forward(addr, w, req)
	if err != nil {
		t.Fatalf("Forwarding failed: %s", err)
	}
	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)

	if string(body) != responseText {
		t.Errorf("Expected body %q, got %q", responseText, string(body))
	}
}

// Test: Least connected server selection works
func TestLeastConnectedServerSelection(t *testing.T) {
	s1 := &BackendServer{Address: "s1", ConnCounter: 3, IsHealthy: true}
	s2 := &BackendServer{Address: "s2", ConnCounter: 1, IsHealthy: true}
	s3 := &BackendServer{Address: "s3", ConnCounter: 2, IsHealthy: true}

	serversPool = []*BackendServer{s1, s2, s3}
	selected := getLeastConnectedServer()

	if selected != s2 {
		t.Errorf("Expected s2 to be selected, got %v", selected)
	}
}

// Test: Connection counter increments and decrements
func TestForwardWithCounter(t *testing.T) {
	mockBackend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}))
	defer mockBackend.Close()

	addr := mockBackend.Listener.Addr().String()
	server := &BackendServer{Address: addr, IsHealthy: true}
	req := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()

	before := atomic.LoadInt32(&server.ConnCounter)
	forwardWithCounter(server, w, req)
	after := atomic.LoadInt32(&server.ConnCounter)

	if before != after {
		t.Errorf("ConnCounter did not reset correctly, before: %d, after: %d", before, after)
	}
}
