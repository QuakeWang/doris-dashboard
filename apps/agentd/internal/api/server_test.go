package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/QuakeWang/doris-dashboard/apps/agentd/internal/doris"
)

type errBody struct {
	OK      bool   `json:"ok"`
	TraceID string `json:"traceId"`
	Error struct {
		Message string `json:"message"`
	} `json:"error"`
}

const (
	localRemoteAddr = "127.0.0.1:12345"
	exportPath      = "/api/v1/doris/audit-log/export"
	connTestPath    = "/api/v1/doris/connection/test"
	databasesPath   = "/api/v1/doris/databases"
	explainPath     = "/api/v1/doris/explain"
	explainTreePath = "/api/v1/doris/explain/tree"
	connTestBody    = `{"connection":{"host":"127.0.0.1","port":19030,"user":"test_user","password":"test_password"}}`
	connWithDBBody  = `{"connection":{"host":"127.0.0.1","port":19030,"user":"test_user","password":"test_password","database":"tpch"}}`
	exportBody      = `{"connection":{"host":"127.0.0.1","port":19030,"user":"test_user","password":"test_password"},"lookbackSeconds":60,"limit":10}`
	explainTreeBody = `{"connection":{"host":"127.0.0.1","port":19030,"user":"test_user","password":"test_password"},"sql":"SELECT 1"}`
)

func newLocalRequest(method, target string, body io.Reader) *http.Request {
	r := httptest.NewRequest(method, target, body)
	r.RemoteAddr = localRemoteAddr
	return r
}

func newLocalJSONRequest(method, target, body string) *http.Request {
	r := newLocalRequest(method, target, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}

func decodeErrBody(t *testing.T, w *httptest.ResponseRecorder) errBody {
	t.Helper()
	var body errBody
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("decode response failed: %v", err)
	}
	return body
}

func assertErrContains(t *testing.T, w *httptest.ResponseRecorder, status int, wantSubstr string) {
	t.Helper()
	if w.Code != status {
		t.Fatalf("unexpected status: %d", w.Code)
	}
	body := decodeErrBody(t, w)
	if body.OK {
		t.Fatalf("unexpected ok=true")
	}
	if body.TraceID == "" {
		t.Fatalf("missing traceId")
	}
	if !strings.Contains(body.Error.Message, wantSubstr) {
		t.Fatalf("unexpected error message: %q", body.Error.Message)
	}
}

func TestServerErrorResponses(t *testing.T) {
	t.Parallel()

	noOpExporter := func(context.Context, doris.ConnConfig, int, int, io.Writer) error { return nil }
	cases := []struct {
		name            string
		handler         http.Handler
		req             *http.Request
		wantStatus      int
		wantErrContains string
	}{
		{
			name:            "export missing connection",
			handler:         NewServer(noOpExporter, 0),
			req:             newLocalJSONRequest(http.MethodPost, exportPath, `{"lookbackSeconds":1,"limit":1}`),
			wantStatus:      http.StatusBadRequest,
			wantErrContains: "connection",
		},
		{
			name: "exporter error writes JSON",
			handler: NewServer(func(context.Context, doris.ConnConfig, int, int, io.Writer) error {
				return errors.New("boom")
			}, 0),
			req:             newLocalJSONRequest(http.MethodPost, exportPath, exportBody),
			wantStatus:      http.StatusBadRequest,
			wantErrContains: "boom",
		},
		{
			name:    "reject non-loopback remote addr",
			handler: NewServer(noOpExporter, 0),
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
				r.RemoteAddr = "192.0.2.1:12345"
				return r
			}(),
			wantStatus:      http.StatusForbidden,
			wantErrContains: "loopback only",
		},
		{
			name:    "reject disallowed origin",
			handler: NewServer(noOpExporter, 0),
			req: func() *http.Request {
				r := newLocalRequest(http.MethodGet, "/api/v1/health", nil)
				r.Header.Set("Origin", "https://evil.invalid")
				return r
			}(),
			wantStatus:      http.StatusForbidden,
			wantErrContains: "origin not allowed",
		},
		{
			name:    "reject non-json content type",
			handler: NewServer(noOpExporter, 0),
			req: func() *http.Request {
				r := newLocalRequest(http.MethodPost, connTestPath, strings.NewReader(connTestBody))
				r.Header.Set("Content-Type", "text/plain")
				return r
			}(),
			wantStatus:      http.StatusBadRequest,
			wantErrContains: "Content-Type must be application/json",
		},
		{
			name:            "explain missing sql",
			handler:         newServer(noOpExporter, 0, nil, func(context.Context, doris.ConnConfig, string, string) (string, error) { return "", nil }, nil),
			req:             newLocalJSONRequest(http.MethodPost, explainPath, connTestBody),
			wantStatus:      http.StatusBadRequest,
			wantErrContains: "sql is required",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			tc.handler.ServeHTTP(w, tc.req)
			assertErrContains(t, w, tc.wantStatus, tc.wantErrContains)
		})
	}
}

func TestExportAuditLogExporterErrorAfterWriteAborts(t *testing.T) {
	t.Parallel()

	h := NewServer(func(ctx context.Context, cfg doris.ConnConfig, lookbackSeconds int, limit int, w io.Writer) error {
		_, _ = io.WriteString(w, "a\tb\n")
		return errors.New("boom")
	}, 0)

	r := newLocalJSONRequest(http.MethodPost, exportPath, exportBody)
	w := httptest.NewRecorder()

	defer func() {
		t.Helper()
		if v := recover(); v != http.ErrAbortHandler {
			t.Fatalf("unexpected panic: %v", v)
		}
	}()
	h.ServeHTTP(w, r)
	t.Fatalf("expected abort")
}

func TestExportAuditLogCallsExporter(t *testing.T) {
	t.Parallel()

	var gotCfg doris.ConnConfig
	var gotLookback int
	var gotLimit int
	h := NewServer(func(ctx context.Context, cfg doris.ConnConfig, lookbackSeconds int, limit int, w io.Writer) error {
		gotCfg = cfg
		gotLookback = lookbackSeconds
		gotLimit = limit
		_, _ = io.WriteString(w, "a\tb\n")
		return nil
	}, 0)

	r := newLocalJSONRequest(http.MethodPost, exportPath, exportBody)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "text/tab-separated-values") {
		t.Fatalf("unexpected content-type: %q", ct)
	}
	if gotCfg.Host != "127.0.0.1" || gotCfg.Port != 19030 || gotCfg.User != "test_user" || gotCfg.Password != "test_password" {
		t.Fatalf("unexpected cfg: %+v", gotCfg)
	}
	if gotLookback != 60 || gotLimit != 10 {
		t.Fatalf("unexpected args: lookback=%d limit=%d", gotLookback, gotLimit)
	}
}

func TestExplainTreeCallsRunner(t *testing.T) {
	t.Parallel()

	var gotCfg doris.ConnConfig
	var gotSQL string
	var gotMode string
	h := newServer(nil, 0, nil, func(ctx context.Context, cfg doris.ConnConfig, sql string, mode string) (string, error) {
		gotCfg = cfg
		gotSQL = sql
		gotMode = mode
		return "[00]:[0: ResultSink]||[Fragment: 0]||", nil
	}, nil)

	r := newLocalJSONRequest(http.MethodPost, explainPath, explainTreeBody)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	}
	if gotCfg.Host != "127.0.0.1" || gotCfg.Port != 19030 || gotCfg.User != "test_user" || gotCfg.Password != "test_password" {
		t.Fatalf("unexpected cfg: %+v", gotCfg)
	}
	if gotSQL != "SELECT 1" {
		t.Fatalf("unexpected sql: %q", gotSQL)
	}
	if gotMode != "tree" {
		t.Fatalf("unexpected mode: %q", gotMode)
	}
	if !strings.Contains(w.Body.String(), `"data":`) {
		t.Fatalf("unexpected response body (missing data envelope): %q", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"rawText":"[00]:[0: ResultSink]||[Fragment: 0]||"`) {
		t.Fatalf("unexpected response body: %q", w.Body.String())
	}
}

func TestExplainPlanCallsRunner(t *testing.T) {
	t.Parallel()

	var gotCfg doris.ConnConfig
	var gotSQL string
	var gotMode string
	h := newServer(nil, 0, nil, func(ctx context.Context, cfg doris.ConnConfig, sql string, mode string) (string, error) {
		gotCfg = cfg
		gotSQL = sql
		gotMode = mode
		return "PLAN FRAGMENT 0", nil
	}, nil)

	body := `{"connection":{"host":"127.0.0.1","port":19030,"user":"test_user","password":"test_password"},"sql":"SELECT 1","mode":"plan"}`
	r := newLocalJSONRequest(http.MethodPost, explainPath, body)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	}
	if gotCfg.Host != "127.0.0.1" || gotCfg.Port != 19030 || gotCfg.User != "test_user" || gotCfg.Password != "test_password" {
		t.Fatalf("unexpected cfg: %+v", gotCfg)
	}
	if gotSQL != "SELECT 1" {
		t.Fatalf("unexpected sql: %q", gotSQL)
	}
	if gotMode != "plan" {
		t.Fatalf("unexpected mode: %q", gotMode)
	}
	if !strings.Contains(w.Body.String(), `"data":`) {
		t.Fatalf("unexpected response body (missing data envelope): %q", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"rawText":"PLAN FRAGMENT 0"`) {
		t.Fatalf("unexpected response body: %q", w.Body.String())
	}
}

func TestListDatabasesCallsRunner(t *testing.T) {
	t.Parallel()

	var gotCfg doris.ConnConfig
	h := newServer(nil, 0, nil, nil, func(ctx context.Context, cfg doris.ConnConfig) ([]string, error) {
		gotCfg = cfg
		return []string{"db1", "db2"}, nil
	})

	r := newLocalJSONRequest(http.MethodPost, databasesPath, connWithDBBody)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	}
	if gotCfg.Database != "tpch" {
		t.Fatalf("unexpected database: %q", gotCfg.Database)
	}
	if !strings.Contains(w.Body.String(), `"data":`) {
		t.Fatalf("unexpected response body (missing data envelope): %q", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"databases":["db1","db2"]`) {
		t.Fatalf("unexpected response body: %q", w.Body.String())
	}
}
