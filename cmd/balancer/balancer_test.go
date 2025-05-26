// file: cmd/balancer/balancer_test.go
package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestScheme(t *testing.T) {
	orig := *https
	defer func() { *https = orig }()

	*https = false
	assert.Equal(t, "http", scheme(), "scheme() should return 'http' when https=false")

	*https = true
	assert.Equal(t, "https", scheme(), "scheme() should return 'https' when https=true")
}

func TestHealth_OK(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer ts.Close()

	host := strings.TrimPrefix(ts.URL, "http://")
	assert.True(t, health(host), "health() should return true for 200 OK")
}

func TestHealth_NotOK(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	host := strings.TrimPrefix(ts.URL, "http://")
	assert.False(t, health(host), "health() should return false for 500 Internal Server Error")
}

func TestHealth_Error(t *testing.T) {
	assert.False(t, health("localhost:0"), "health() should return false on connection error")
}

func TestGetLeastConnectedServer_NoneHealthy(t *testing.T) {
	orig := serversPool
	defer func() { serversPool = orig }()

	serversPool = []*BackendServer{
		{Address: "a", ConnCounter: 0, IsHealthy: false},
		{Address: "b", ConnCounter: 0, IsHealthy: false},
	}
	assert.Nil(t, getLeastConnectedServer(), "should return nil when no healthy servers are available")
}

func TestGetLeastConnectedServer_SelectLowest(t *testing.T) {
	orig := serversPool
	defer func() { serversPool = orig }()

	serversPool = []*BackendServer{
		{Address: "a", ConnCounter: 5, IsHealthy: true},
		{Address: "b", ConnCounter: 3, IsHealthy: true},
		{Address: "c", ConnCounter: 10, IsHealthy: true},
	}
	srv := getLeastConnectedServer()
	assert.NotNil(t, srv)
	assert.Equal(t, "b", srv.Address, "should select the server with the fewest connections")
}

func TestGetLeastConnectedServer_SkipUnhealthy(t *testing.T) {
	orig := serversPool
	defer func() { serversPool = orig }()

	serversPool = []*BackendServer{
		{Address: "a", ConnCounter: 1, IsHealthy: false},
		{Address: "b", ConnCounter: 0, IsHealthy: true},
	}
	srv := getLeastConnectedServer()
	assert.NotNil(t, srv)
	assert.Equal(t, "b", srv.Address, "should skip unhealthy servers")
}

func TestForward_SuccessAndTrace(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Test", "ok")
		w.WriteHeader(http.StatusTeapot)
		io.WriteString(w, "body123")
	}))
	defer backend.Close()

	host := strings.TrimPrefix(backend.URL, "http://")
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)

	*traceEnabled = false
	err := forward(host, rr, req)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusTeapot, rr.Code)
	assert.Equal(t, "ok", rr.Header().Get("X-Test"))
	assert.Empty(t, rr.Header().Get("lb-from"))
	assert.Equal(t, "body123", rr.Body.String())

	*traceEnabled = true
	rr = httptest.NewRecorder()
	err = forward(host, rr, req)
	assert.NoError(t, err)
	assert.Equal(t, host, rr.Header().Get("lb-from"), "should set lb-from header when traceEnabled is true")
}

func TestForward_Error(t *testing.T) {
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)

	err := forward("localhost:0", rr, req)
	assert.Error(t, err)
	assert.Equal(t, http.StatusServiceUnavailable, rr.Code)
}

func TestForwardWithCounter(t *testing.T) {
	mock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer mock.Close()

	addr := strings.TrimPrefix(mock.URL, "http://")
	server := &BackendServer{Address: addr, IsHealthy: true}

	before := atomic.LoadInt32(&server.ConnCounter)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)

	forwardWithCounter(server, rr, req)
	after := atomic.LoadInt32(&server.ConnCounter)
	assert.Equal(t, before, after, "ConnCounter should return to its initial value after forwarding")
}