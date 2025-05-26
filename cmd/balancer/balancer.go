package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

var (
	port       = flag.Int("port", 8090, "load balancer port")
	timeoutSec = flag.Int("timeout-sec", 3, "request timeout time in seconds")
	https      = flag.Bool("https", false, "whether backends support HTTPs")

	traceEnabled = flag.Bool("trace", false, "whether to include tracing information into responses")
)

type BackendServer struct {
	Address     string
	ConnCounter int32
	IsHealthy   bool
}

var (
	timeout     = time.Duration(*timeoutSec) * time.Second
	serversPool = []*BackendServer{
		{Address: "server1:8080"},
		{Address: "server2:8080"},
		{Address: "server3:8080"},
	}
)

func scheme() string {
	if *https {
		return "https"
	}
	return "http"
}

func health(dst string) bool {
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s://%s/health", scheme(), dst), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	return true
}

func forward(dst string, writer http.ResponseWriter, req *http.Request) error {
	ctx, _ := context.WithTimeout(req.Context(), timeout)
	fwdRequest := req.Clone(ctx)
	fwdRequest.RequestURI = ""
	fwdRequest.URL.Host = dst
	fwdRequest.URL.Scheme = scheme()
	fwdRequest.Host = dst

	resp, err := http.DefaultClient.Do(fwdRequest)
	if err == nil {
		for k, values := range resp.Header {
			for _, value := range values {
				writer.Header().Add(k, value)
			}
		}
		if *traceEnabled {
			writer.Header().Set("lb-from", dst)
		}
		log.Println("fwd", resp.StatusCode, resp.Request.URL)
		writer.WriteHeader(resp.StatusCode)
		defer resp.Body.Close()
		_, err := io.Copy(writer, resp.Body)
		if err != nil {
			log.Printf("Failed to write response: %s", err)
		}
		return nil
	} else {
		log.Printf("Failed to get response from %s: %s", dst, err)
		writer.WriteHeader(http.StatusServiceUnavailable)
		return err
	}
}

func getLeastConnectedServer() *BackendServer {
	var selected *BackendServer
	var minConns int32 = math.MaxInt32

	for _, server := range serversPool {
		if !server.IsHealthy {
			continue
		}

		current := atomic.LoadInt32(&server.ConnCounter)
		if current < minConns {
			minConns = current
			selected = server
		}
	}
	return selected
}

func forwardWithCounter(server *BackendServer, w http.ResponseWriter, r *http.Request) {
	atomic.AddInt32(&server.ConnCounter, 1)
	defer atomic.AddInt32(&server.ConnCounter, -1)

	forward(server.Address, w, r)
}

func main() {
	flag.Parse()

	for _, server := range serversPool {
		go func() {
			for range time.Tick(10 * time.Second) {
				server.IsHealthy = true
				log.Println(server, "healthy:", health(server.Address))
			}
		}()
	}

	frontend := httptools.CreateServer(*port, http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		selectedServer := getLeastConnectedServer()
		if selectedServer == nil {
			http.Error(writer, "No available backend server", http.StatusServiceUnavailable)
			return
		}
		forwardWithCounter(selectedServer, writer, req)
	}))

	log.Println("Starting load balancer...")
	log.Printf("Tracing support enabled: %t", *traceEnabled)
	frontend.Start()
	signal.WaitForTerminationSignal()
}
