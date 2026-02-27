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
	"github.com/go-sql-driver/mysql"
)

type errBody struct {
	OK      bool   `json:"ok"`
	TraceID string `json:"traceId"`
	Error   struct {
		Message string `json:"message"`
	} `json:"error"`
}

const (
	localRemoteAddr            = "127.0.0.1:12345"
	exportPath                 = "/api/v1/doris/audit-log/export"
	connTestPath               = "/api/v1/doris/connection/test"
	databasesPath              = "/api/v1/doris/databases"
	explainPath                = "/api/v1/doris/explain"
	schemaAuditScanPath        = "/api/v1/doris/schema-audit/scan"
	schemaAuditTableDetailPath = "/api/v1/doris/schema-audit/table-detail"
	connTestBody               = `{"connection":{"host":"127.0.0.1","port":19030,"user":"test_user","password":"test_password"}}`
	connWithDBBody             = `{"connection":{"host":"127.0.0.1","port":19030,"user":"test_user","password":"test_password","database":"tpch"}}`
	exportBody                 = `{"connection":{"host":"127.0.0.1","port":19030,"user":"test_user","password":"test_password"},"lookbackSeconds":60,"limit":10}`
	explainTreeBody            = `{"connection":{"host":"127.0.0.1","port":19030,"user":"test_user","password":"test_password"},"sql":"SELECT 1"}`
	schemaAuditScanBody        = `{"connection":{"host":"127.0.0.1","port":19030,"user":"test_user","password":"test_password"},"database":"db1","tableLike":"fact","page":2,"pageSize":10}`
	schemaAuditTableDetailBody = `{"connection":{"host":"127.0.0.1","port":19030,"user":"test_user","password":"test_password"},"database":"db1","table":"tbl1"}`
	schemaAuditTableDetailNoDB = `{"connection":{"host":"127.0.0.1","port":19030,"user":"test_user","password":"test_password"},"table":"tbl1"}`
	schemaAuditTableDetailConn = `{"connection":{"host":"127.0.0.1","port":19030,"user":"test_user","password":"test_password","database":"tpch"},"table":"tbl1"}`
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

func serveLocalJSON(handler http.Handler, method, target, body string) *httptest.ResponseRecorder {
	r := newLocalJSONRequest(method, target, body)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
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
	assertStatus(t, w, status)
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

func assertDefaultConn(t *testing.T, got doris.ConnConfig) {
	t.Helper()
	if got.Host != "127.0.0.1" || got.Port != 19030 || got.User != "test_user" || got.Password != "test_password" {
		t.Fatalf("unexpected cfg: %+v", got)
	}
}

func assertStatus(t *testing.T, w *httptest.ResponseRecorder, want int) {
	t.Helper()
	if w.Code != want {
		t.Fatalf("unexpected status: %d", w.Code)
	}
}

func assertBodyContains(t *testing.T, w *httptest.ResponseRecorder, wantSubstr string) {
	t.Helper()
	if !strings.Contains(w.Body.String(), wantSubstr) {
		t.Fatalf("unexpected response body: %q", w.Body.String())
	}
}

func newTestServer(
	exporter AuditLogExporter,
	testConnection TestConnectionRunner,
	explain ExplainRunner,
	listDatabases ListDatabasesRunner,
	schemaAuditScan SchemaAuditScanRunner,
	schemaAuditTableDetail SchemaAuditTableDetailRunner,
) http.Handler {
	return newServer(
		exporter,
		0,
		testConnection,
		explain,
		listDatabases,
		schemaAuditScan,
		schemaAuditTableDetail,
	)
}

func newTestServerWithConnectionRunner(runner TestConnectionRunner) http.Handler {
	return newTestServer(nil, runner, nil, nil, nil, nil)
}

func newTestServerWithExplainRunner(runner ExplainRunner) http.Handler {
	return newTestServer(nil, nil, runner, nil, nil, nil)
}

func newTestServerWithDatabasesRunner(runner ListDatabasesRunner) http.Handler {
	return newTestServer(nil, nil, nil, runner, nil, nil)
}

func newTestServerWithSchemaAuditScanRunner(runner SchemaAuditScanRunner) http.Handler {
	return newTestServer(nil, nil, nil, nil, runner, nil)
}

func newTestServerWithSchemaAuditTableDetailRunner(
	runner SchemaAuditTableDetailRunner,
) http.Handler {
	return newTestServer(nil, nil, nil, nil, nil, runner)
}

func TestServerErrorResponses(t *testing.T) {
	t.Parallel()

	noOpExporter := func(context.Context, doris.ConnConfig, int, int, io.Writer) error { return nil }
	defaultHandler := NewServer(noOpExporter, 0)
	postJSON := func(path, body string) *http.Request {
		return newLocalJSONRequest(http.MethodPost, path, body)
	}
	cases := []struct {
		name            string
		handler         http.Handler
		req             *http.Request
		wantStatus      int
		wantErrContains string
	}{
		{
			name:            "export missing connection",
			handler:         defaultHandler,
			req:             postJSON(exportPath, `{"lookbackSeconds":1,"limit":1}`),
			wantStatus:      http.StatusBadRequest,
			wantErrContains: "connection",
		},
		{
			name: "exporter error writes JSON",
			handler: NewServer(func(context.Context, doris.ConnConfig, int, int, io.Writer) error {
				return errors.New("boom")
			}, 0),
			req:             postJSON(exportPath, exportBody),
			wantStatus:      http.StatusBadRequest,
			wantErrContains: "boom",
		},
		{
			name:    "reject non-loopback remote addr",
			handler: defaultHandler,
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
			handler: defaultHandler,
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
			handler: defaultHandler,
			req: func() *http.Request {
				r := newLocalRequest(http.MethodPost, connTestPath, strings.NewReader(connTestBody))
				r.Header.Set("Content-Type", "text/plain")
				return r
			}(),
			wantStatus:      http.StatusBadRequest,
			wantErrContains: "Content-Type must be application/json",
		},
		{
			name: "explain missing sql",
			handler: newTestServer(
				noOpExporter,
				nil,
				func(context.Context, doris.ConnConfig, string, string) (string, error) { return "", nil },
				nil,
				nil,
				nil,
			),
			req:             postJSON(explainPath, connTestBody),
			wantStatus:      http.StatusBadRequest,
			wantErrContains: "sql is required",
		},
		{
			name:            "schema audit table detail missing database",
			handler:         defaultHandler,
			req:             postJSON(schemaAuditTableDetailPath, schemaAuditTableDetailNoDB),
			wantStatus:      http.StatusBadRequest,
			wantErrContains: "database is required",
		},
		{
			name:            "schema audit table detail requires request database even when connection has database",
			handler:         defaultHandler,
			req:             postJSON(schemaAuditTableDetailPath, schemaAuditTableDetailConn),
			wantStatus:      http.StatusBadRequest,
			wantErrContains: "database is required",
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

	w := serveLocalJSON(h, http.MethodPost, exportPath, exportBody)
	assertStatus(t, w, http.StatusOK)
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "text/tab-separated-values") {
		t.Fatalf("unexpected content-type: %q", ct)
	}
	assertDefaultConn(t, gotCfg)
	if gotLookback != 60 || gotLimit != 10 {
		t.Fatalf("unexpected args: lookback=%d limit=%d", gotLookback, gotLimit)
	}
}

func TestConnectionTestCallsRunner(t *testing.T) {
	t.Parallel()

	var gotCfg doris.ConnConfig
	h := newTestServerWithConnectionRunner(func(_ context.Context, cfg doris.ConnConfig) error {
		gotCfg = cfg
		return nil
	})

	w := serveLocalJSON(h, http.MethodPost, connTestPath, connTestBody)
	assertStatus(t, w, http.StatusOK)
	assertDefaultConn(t, gotCfg)
	assertBodyContains(t, w, `"connected":true`)
}

func TestConnectionTestRunnerError(t *testing.T) {
	t.Parallel()

	h := newTestServerWithConnectionRunner(func(context.Context, doris.ConnConfig) error {
		return errors.New("connection probe failed")
	})

	w := serveLocalJSON(h, http.MethodPost, connTestPath, connTestBody)
	assertErrContains(t, w, http.StatusBadRequest, "connection probe failed")
}

func TestExplainCallsRunner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantMode string
		rawText  string
	}{
		{
			name:     "tree mode by default",
			body:     explainTreeBody,
			wantMode: "tree",
			rawText:  "[00]:[0: ResultSink]||[Fragment: 0]||",
		},
		{
			name:     "plan mode",
			body:     `{"connection":{"host":"127.0.0.1","port":19030,"user":"test_user","password":"test_password"},"sql":"SELECT 1","mode":"plan"}`,
			wantMode: "plan",
			rawText:  "PLAN FRAGMENT 0",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var gotCfg doris.ConnConfig
			var gotSQL string
			var gotMode string
			h := newTestServerWithExplainRunner(func(
				ctx context.Context,
				cfg doris.ConnConfig,
				sql string,
				mode string,
			) (string, error) {
				gotCfg = cfg
				gotSQL = sql
				gotMode = mode
				return tc.rawText, nil
			})

			w := serveLocalJSON(h, http.MethodPost, explainPath, tc.body)
			assertStatus(t, w, http.StatusOK)
			assertDefaultConn(t, gotCfg)
			if gotSQL != "SELECT 1" {
				t.Fatalf("unexpected sql: %q", gotSQL)
			}
			if gotMode != tc.wantMode {
				t.Fatalf("unexpected mode: %q", gotMode)
			}
			assertBodyContains(t, w, `"rawText":"`+tc.rawText+`"`)
		})
	}
}

func TestListDatabasesCallsRunner(t *testing.T) {
	t.Parallel()

	var gotCfg doris.ConnConfig
	h := newTestServerWithDatabasesRunner(func(
		ctx context.Context,
		cfg doris.ConnConfig,
	) ([]string, error) {
		gotCfg = cfg
		return []string{"db1", "db2"}, nil
	})

	w := serveLocalJSON(h, http.MethodPost, databasesPath, connWithDBBody)
	assertStatus(t, w, http.StatusOK)
	assertDefaultConn(t, gotCfg)
	if gotCfg.Database != "tpch" {
		t.Fatalf("unexpected database: %q", gotCfg.Database)
	}
	assertBodyContains(t, w, `"databases":["db1","db2"]`)
}

func TestSchemaAuditScanCallsRunner(t *testing.T) {
	t.Parallel()

	var gotCfg doris.ConnConfig
	var gotOptions doris.SchemaAuditScanOptions
	h := newTestServerWithSchemaAuditScanRunner(func(
		ctx context.Context,
		cfg doris.ConnConfig,
		opts doris.SchemaAuditScanOptions,
	) (doris.SchemaAuditScanResult, error) {
		gotCfg = cfg
		gotOptions = opts
		return doris.SchemaAuditScanResult{
			Inventory: doris.SchemaAuditInventory{TableCount: 1},
			Items: []doris.SchemaAuditScanItem{
				{
					Database: "db1",
					Table:    "tbl1",
				},
			},
			Page:       2,
			PageSize:   10,
			TotalItems: 1,
		}, nil
	})

	w := serveLocalJSON(h, http.MethodPost, schemaAuditScanPath, schemaAuditScanBody)
	assertStatus(t, w, http.StatusOK)
	assertDefaultConn(t, gotCfg)
	if gotOptions.Database != "db1" || gotOptions.TableLike != "fact" {
		t.Fatalf("unexpected opts: %+v", gotOptions)
	}
	if gotOptions.Page != 2 || gotOptions.PageSize != 10 {
		t.Fatalf("unexpected pagination opts: %+v", gotOptions)
	}
	assertBodyContains(t, w, `"tableCount":1`)
}

func TestSchemaAuditTableDetailCallsRunner(t *testing.T) {
	t.Parallel()

	var gotCfg doris.ConnConfig
	var gotDatabase string
	var gotTable string
	h := newTestServerWithSchemaAuditTableDetailRunner(func(
		ctx context.Context,
		cfg doris.ConnConfig,
		database string,
		table string,
	) (doris.SchemaAuditTableDetailResult, error) {
		gotCfg = cfg
		gotDatabase = database
		gotTable = table
		return doris.SchemaAuditTableDetailResult{
			Database:       database,
			Table:          table,
			CreateTableSQL: "CREATE TABLE ...",
		}, nil
	})

	w := serveLocalJSON(h, http.MethodPost, schemaAuditTableDetailPath, schemaAuditTableDetailBody)
	assertStatus(t, w, http.StatusOK)
	assertDefaultConn(t, gotCfg)
	if gotDatabase != "db1" || gotTable != "tbl1" {
		t.Fatalf("unexpected target: %s.%s", gotDatabase, gotTable)
	}
	assertBodyContains(t, w, `"createTableSql":"CREATE TABLE ..."`)
}

func TestSchemaAuditScanRunnerErrorStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		runnerErr  error
		wantStatus int
	}{
		{
			name:       "validation error returns bad request",
			runnerErr:  errors.New("database filter is invalid"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "upstream error returns bad gateway",
			runnerErr:  errors.New("dial tcp 10.0.0.1:9030: i/o timeout"),
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "mysql unknown database returns bad request",
			runnerErr:  &mysql.MySQLError{Number: 1049, Message: "Unknown database 'db_not_exists'"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "mysql unknown error with unknown table detail returns bad request",
			runnerErr:  &mysql.MySQLError{Number: 1105, Message: "detailMessage = Unknown table 'db1.tbl_not_exists'"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "mysql unknown error without object detail returns bad gateway",
			runnerErr:  &mysql.MySQLError{Number: 1105, Message: "detailMessage = rpc timeout while fetching metadata"},
			wantStatus: http.StatusBadGateway,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestServerWithSchemaAuditScanRunner(func(
				context.Context,
				doris.ConnConfig,
				doris.SchemaAuditScanOptions,
			) (doris.SchemaAuditScanResult, error) {
				return doris.SchemaAuditScanResult{}, tc.runnerErr
			})

			w := serveLocalJSON(h, http.MethodPost, schemaAuditScanPath, schemaAuditScanBody)

			assertErrContains(t, w, tc.wantStatus, tc.runnerErr.Error())
		})
	}
}

func TestSchemaAuditTableDetailRunnerErrorStatus(t *testing.T) {
	t.Parallel()

	h := newTestServerWithSchemaAuditTableDetailRunner(func(
		context.Context,
		doris.ConnConfig,
		string,
		string,
	) (doris.SchemaAuditTableDetailResult, error) {
		return doris.SchemaAuditTableDetailResult{}, errors.New("query execution failed")
	})

	w := serveLocalJSON(h, http.MethodPost, schemaAuditTableDetailPath, schemaAuditTableDetailBody)

	assertErrContains(t, w, http.StatusBadGateway, "query execution failed")
}

func TestSchemaAuditTableDetailRunnerMySQLRequestErrorStatus(t *testing.T) {
	t.Parallel()

	h := newTestServerWithSchemaAuditTableDetailRunner(func(
		context.Context,
		doris.ConnConfig,
		string,
		string,
	) (doris.SchemaAuditTableDetailResult, error) {
		return doris.SchemaAuditTableDetailResult{}, &mysql.MySQLError{
			Number:  1146,
			Message: "Table 'db1.tbl_not_exists' doesn't exist",
		}
	})

	w := serveLocalJSON(h, http.MethodPost, schemaAuditTableDetailPath, schemaAuditTableDetailBody)

	assertErrContains(t, w, http.StatusBadRequest, "doesn't exist")
}
