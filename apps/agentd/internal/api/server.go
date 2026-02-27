package api

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/QuakeWang/doris-dashboard/apps/agentd/internal/doris"
)

type AuditLogExporter func(
	ctx context.Context,
	cfg doris.ConnConfig,
	lookbackSeconds int,
	limit int,
	w io.Writer,
) error

type ExplainRunner func(ctx context.Context, cfg doris.ConnConfig, sql string, mode string) (string, error)

type ListDatabasesRunner func(ctx context.Context, cfg doris.ConnConfig) ([]string, error)

type TestConnectionRunner func(ctx context.Context, cfg doris.ConnConfig) error

type SchemaAuditScanRunner func(
	ctx context.Context,
	cfg doris.ConnConfig,
	opts doris.SchemaAuditScanOptions,
) (doris.SchemaAuditScanResult, error)

type SchemaAuditTableDetailRunner func(
	ctx context.Context,
	cfg doris.ConnConfig,
	database string,
	table string,
) (doris.SchemaAuditTableDetailResult, error)

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

type dorisConnection struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database,omitempty"`
}

func parseConnConfig(c *dorisConnection) (doris.ConnConfig, error) {
	if c == nil {
		return doris.ConnConfig{}, errors.New("connection is required")
	}
	host := strings.TrimSpace(c.Host)
	user := strings.TrimSpace(c.User)
	if host == "" {
		return doris.ConnConfig{}, errors.New("connection.host is required")
	}
	if c.Port <= 0 || c.Port > 65535 {
		return doris.ConnConfig{}, errors.New("connection.port must be in 1..65535")
	}
	if user == "" {
		return doris.ConnConfig{}, errors.New("connection.user is required")
	}
	if c.Password == "" {
		return doris.ConnConfig{}, errors.New("connection.password is required")
	}
	database := strings.TrimSpace(c.Database)
	if database != "" {
		if strings.HasPrefix(database, "`") && strings.HasSuffix(database, "`") && len(database) >= 2 {
			database = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(database, "`"), "`"))
		}
		if database == "" {
			return doris.ConnConfig{}, errors.New("connection.database is invalid")
		}
		if strings.ContainsAny(database, "`;\r\n\t ") {
			return doris.ConnConfig{}, errors.New("connection.database must be a database name (no quotes or semicolons)")
		}
	}
	return doris.ConnConfig{
		Host:     host,
		Port:     c.Port,
		User:     user,
		Password: c.Password,
		Database: database,
	}, nil
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method != method {
		writeErrorWithRequest(w, r, http.StatusMethodNotAllowed, "method not allowed")
		return false
	}
	return true
}

func readJSONOrWriteError(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := readJSON(w, r, dst); err != nil {
		writeErrorWithRequest(w, r, http.StatusBadRequest, err.Error())
		return false
	}
	return true
}

func parseConnConfigOrWriteError(
	w http.ResponseWriter,
	r *http.Request,
	c *dorisConnection,
) (doris.ConnConfig, bool) {
	cfg, err := parseConnConfig(c)
	if err != nil {
		writeErrorWithRequest(w, r, http.StatusBadRequest, err.Error())
		return doris.ConnConfig{}, false
	}
	return cfg, true
}

func applyReadWriteTimeout(cfg *doris.ConnConfig, timeout time.Duration) {
	cfg.ReadTimeout = timeout
	cfg.WriteTimeout = timeout
}

func normalizeExplainMode(mode string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(mode))
	switch normalized {
	case "", "tree":
		return "tree", nil
	case "plan":
		return "plan", nil
	default:
		return "", errors.New("unsupported explain mode: " + normalized)
	}
}

type Server struct {
	exportAuditLog         AuditLogExporter
	testConnection         TestConnectionRunner
	explain                ExplainRunner
	listDatabases          ListDatabasesRunner
	schemaAuditScan        SchemaAuditScanRunner
	schemaAuditTableDetail SchemaAuditTableDetailRunner
	exportTimeout          time.Duration
}

func NewServer(
	exporter AuditLogExporter,
	exportTimeout time.Duration,
	testConnection ...TestConnectionRunner,
) http.Handler {
	tc := doris.TestConnection
	if len(testConnection) > 0 && testConnection[0] != nil {
		tc = testConnection[0]
	}
	return newServer(exporter, exportTimeout, tc, nil, nil, nil, nil)
}

func newServer(
	exporter AuditLogExporter,
	exportTimeout time.Duration,
	testConnection TestConnectionRunner,
	explain ExplainRunner,
	listDatabases ListDatabasesRunner,
	schemaAuditScan SchemaAuditScanRunner,
	schemaAuditTableDetail SchemaAuditTableDetailRunner,
) http.Handler {
	if exporter == nil {
		exporter = doris.StreamAuditLogOutfileTSVLookback
	}
	if exportTimeout <= 0 {
		exportTimeout = 60 * time.Second
	}
	if testConnection == nil {
		testConnection = doris.TestConnection
	}
	if explain == nil {
		explain = func(ctx context.Context, cfg doris.ConnConfig, sqlText string, mode string) (string, error) {
			normalizedMode, err := normalizeExplainMode(mode)
			if err != nil {
				return "", err
			}
			if normalizedMode == "plan" {
				return doris.ExplainPlan(ctx, cfg, sqlText)
			}
			return doris.ExplainTree(ctx, cfg, sqlText)
		}
	}
	if listDatabases == nil {
		listDatabases = doris.ListDatabases
	}
	if schemaAuditScan == nil {
		schemaAuditScan = doris.BuildSchemaAuditScan
	}
	if schemaAuditTableDetail == nil {
		schemaAuditTableDetail = doris.BuildSchemaAuditTableDetail
	}

	server := &Server{
		exportAuditLog:         exporter,
		testConnection:         testConnection,
		explain:                explain,
		listDatabases:          listDatabases,
		schemaAuditScan:        schemaAuditScan,
		schemaAuditTableDetail: schemaAuditTableDetail,
		exportTimeout:          exportTimeout,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", server.handleHealth)
	mux.HandleFunc("/api/v1/doris/connection/test", server.handleDorisConnectionTest)
	mux.HandleFunc("/api/v1/doris/databases", server.handleDorisDatabases)
	mux.HandleFunc("/api/v1/doris/audit-log/export", server.handleDorisAuditLogExport)
	mux.HandleFunc("/api/v1/doris/explain", server.handleDorisExplain)
	mux.HandleFunc("/api/v1/doris/explain/tree", server.handleDorisExplain)
	mux.HandleFunc("/api/v1/doris/schema-audit/scan", server.handleDorisSchemaAuditScan)
	mux.HandleFunc("/api/v1/doris/schema-audit/table-detail", server.handleDorisSchemaAuditTableDetail)
	return withLocalOnly(withCORS(mux))
}

func isAllowedOrigin(origin string) bool {
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
	default:
		return false
	}
	host := strings.ToLower(u.Hostname())
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func withLocalOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		ip := net.ParseIP(host)
		if err != nil || ip == nil || !ip.IsLoopback() {
			writeErrorWithRequest(w, r, http.StatusForbidden, "loopback only")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && !isAllowedOrigin(origin) {
			writeErrorWithRequest(w, r, http.StatusForbidden, "origin not allowed")
			return
		}
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
		}
		if r.Method == http.MethodOptions {
			if origin != "" {
				w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
				w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}
