package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/mux"
	"github.com/maxnetyaga/architecture-practice-5/datastore"
)

func main() {
	if err := os.MkdirAll("./data", 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	db, err := datastore.Open("./data", 0)
	if err != nil {
		log.Fatalf("DB init failed: %v", err)
	}

	r := mux.NewRouter()

	r.HandleFunc("/db/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := mux.Vars(r)["key"]
		value, err := db.Get(key)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"key":   key,
			"value": value,
		})
	}).Methods("GET")

	r.HandleFunc("/db/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := mux.Vars(r)["key"]
		var body struct {
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if err := db.Put(key, body.Value); err != nil {
			http.Error(w, "failed to store value", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}).Methods("POST")

	log.Println("Starting DB server on :8083")
	log.Fatal(http.ListenAndServe(":8083", r))
}
