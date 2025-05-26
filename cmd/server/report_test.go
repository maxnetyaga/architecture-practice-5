package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReportProcess_NoAuthor(t *testing.T) {
	r := make(Report)
	req := httptest.NewRequest("GET", "/", nil)
	r.Process(req)
	assert.Empty(t, r, "nothing should be added without lb-author header")
}

func TestReportProcess_AddAndTrim(t *testing.T) {
	r := make(Report)
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("lb-author", "usr")
	req.Header.Set("lb-req-cnt", "1")

	r.Process(req)
	assert.Equal(t, []string{"1"}, r["usr"], "first entry should be added")

	req.Header.Set("lb-req-cnt", "2")
	r.Process(req)
	assert.Equal(t, []string{"1", "2"}, r["usr"], "second entry should be appended")

	for i := 3; i <= reportMaxLen+5; i++ {
		req.Header.Set("lb-req-cnt", fmt.Sprintf("%d", i))
		r.Process(req)
	}
	list := r["usr"]
	assert.Len(t, list, reportMaxLen, "list length should not exceed reportMaxLen")
	assert.Equal(t, fmt.Sprintf("%d", reportMaxLen+5), list[len(list)-1], "last element should be the last added")
}

func TestReportServeHTTP(t *testing.T) {
	orig := Report{
		"alice": {"1", "2"},
		"bob":   {"x"},
	}
	rr := httptest.NewRecorder()

	orig.ServeHTTP(rr, nil)

	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, "application/json", rr.Header().Get("content-type"))

	var got Report
	err := json.Unmarshal(rr.Body.Bytes(), &got)
	assert.NoError(t, err)
	assert.Equal(t, orig, got, "JSON response should exactly match the report")
}