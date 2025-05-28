package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/maxnetyaga/architecture-practice-5/httptools"
	"github.com/maxnetyaga/architecture-practice-5/signal"
)

var port = flag.Int("port", 8080, "server port")

const (
	confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
	confHealthFailure    = "CONF_HEALTH_FAILURE"
	envTeamName          = "TEAM_NAME"
	envDbAddr            = "DB_ADDR"
)

func main() {
	flag.Parse()

	team := os.Getenv(envTeamName)
	if team == "" {
		log.Fatal("Environment variable TEAM_NAME is required")
	}
	dbAddr := os.Getenv(envDbAddr)
	if dbAddr == "" {
		dbAddr = "db:8083"
	}

	dateStr := time.Now().Format("2006-01-02")
	payload, _ := json.Marshal(map[string]string{"value": dateStr})

	resp, err := http.Post(
		fmt.Sprintf("http://%s/db/%s", dbAddr, url.PathEscape(team)),
		"application/json",
		bytes.NewReader(payload),
	)
	if err != nil {
		log.Fatalf("DB init failed (request): %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("DB init failed (status %d): %s", resp.StatusCode, string(body))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthHandler)
	mux.HandleFunc("/api/v1/some-data", someDataHandler(dbAddr))
	mux.Handle("/report", make(Report))

	server := httptools.CreateServer(*port, mux)
	server.Start()
	signal.WaitForTerminationSignal()
}

func healthHandler(rw http.ResponseWriter, r *http.Request) {
	rw.Header().Set("content-type", "text/plain")
	if os.Getenv(confHealthFailure) == "true" {
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte("FAILURE"))
	} else {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("OK"))
	}
}

func someDataHandler(dbAddr string) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		if ds := os.Getenv(confResponseDelaySec); ds != "" {
			if sec, err := strconv.Atoi(ds); err == nil && sec > 0 && sec < 300 {
				time.Sleep(time.Duration(sec) * time.Second)
			}
		}

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(rw, "key required", http.StatusBadRequest)
			return
		}

		dbResp, err := http.Get("http://" + dbAddr + "/db/" + url.PathEscape(key))
		if err != nil {
			http.Error(rw, "error fetching data", http.StatusInternalServerError)
			return
		}
		defer dbResp.Body.Close()

		if dbResp.StatusCode == http.StatusNotFound {
			http.NotFound(rw, r)
			return
		}

		rw.Header().Set("Content-Type", "application/json")
		rw.WriteHeader(http.StatusOK)
		io.Copy(rw, dbResp.Body)
	}
}
