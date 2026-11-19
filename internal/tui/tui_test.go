package tui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"burstkv/internal/core/pod_resources.go"
)

func TestRunActionSendsCorrectRequest(t *testing.T) {
	tests := []struct {
		action, id, wantMethod, wantPath string
	}{
		{"create", "", http.MethodPost, "/database"},
		{"pause", "db1", http.MethodPost, "/database/pause/db1"},
		{"resume", "db1", http.MethodPost, "/database/resume/db1"},
		{"delete", "db1", http.MethodDelete, "/database/delete/db1"},
	}

	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			var gotMethod, gotPath string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotMethod, gotPath = r.Method, r.URL.Path
				w.Write([]byte(`{"success":true}`))
			}))
			defer srv.Close()

			result := runDatabaseAction(srv.Client(), srv.URL, tt.action, tt.id)

			if gotMethod != tt.wantMethod || gotPath != tt.wantPath {
				t.Errorf("got %s %s, want %s %s", gotMethod, gotPath, tt.wantMethod, tt.wantPath)
			}
			if !strings.Contains(result, "200 OK") {
				t.Errorf("result %q doesn't report success", result)
			}
		})
	}
}

func TestRunActionReportsControllerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "database not found", http.StatusNotFound)
	}))
	defer srv.Close()

	result := runDatabaseAction(srv.Client(), srv.URL, "pause", "missing")

	if !strings.Contains(result, "404") || !strings.Contains(result, "database not found") {
		t.Errorf("result %q should contain status and body", result)
	}
}

func TestFetchStatuses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"databases":{"db1":{"Running":true},"db2":{"Running":false}}}`))
	}))
	defer srv.Close()

	statuses, err := fetchStatuses(srv.Client(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if len(statuses) != 2 || !statuses["db1"].Running || statuses["db2"].Running {
		t.Errorf("unexpected statuses: %+v", statuses)
	}
}

func TestFetchStatusesNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	if _, err := fetchStatuses(srv.Client(), srv.URL); err == nil {
		t.Error("expected an error for a 500 response")
	}
}

func TestFormatResources(t *testing.T) {
	used := &pod_resources.PodResources{Cpu: 50, Memory: 64 << 20}
	max := &pod_resources.PodResources{Cpu: 500, Memory: 256 << 20}

	cpu, mem := formatResources(used, max)
	if cpu != "50/500" || mem != "64/256" {
		t.Errorf("got %s, %s", cpu, mem)
	}

	cpu, mem = formatResources(nil, max)
	if cpu != "-" || mem != "-" {
		t.Errorf("nil used should give dashes, got %s, %s", cpu, mem)
	}
}
