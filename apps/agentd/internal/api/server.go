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
	exportAuditLog AuditLogExporter
	queryVersion   func(ctx context.Context, cfg doris.ConnConfig) (string, error)
	explain        ExplainRunner
	listDatabases  ListDatabasesRunner
	exportTimeout  time.Duration
}

func NewServer(
	exporter AuditLogExporter,
	exportTimeout time.Duration,
	queryVersion ...func(ctx context.Context, cfg doris.ConnConfig) (string, error),
) http.Handler {
	qv := doris.QueryVersion
	if len(queryVersion) > 0 && queryVersion[0] != nil {
		qv = queryVersion[0]
	}
	return newServer(exporter, exportTimeout, qv, nil, nil)
}

func newServer(
	exporter AuditLogExporter,
	exportTimeout time.Duration,
	queryVersion func(ctx context.Context, cfg doris.ConnConfig) (string, error),
	explain ExplainRunner,
	listDatabases ListDatabasesRunner,
) http.Handler {
	if exporter == nil {
		exporter = doris.StreamAuditLogOutfileTSVLookback
	}
	if exportTimeout <= 0 {
		exportTimeout = 60 * time.Second
	}
	if queryVersion == nil {
		queryVersion = doris.QueryVersion
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

	server := &Server{
		exportAuditLog: exporter,
		queryVersion:   queryVersion,
		explain:        explain,
		listDatabases:  listDatabases,
		exportTimeout:  exportTimeout,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", server.handleHealth)
	mux.HandleFunc("/api/v1/doris/connection/test", server.handleDorisConnectionTest)
	mux.HandleFunc("/api/v1/doris/databases", server.handleDorisDatabases)
	mux.HandleFunc("/api/v1/doris/audit-log/export", server.handleDorisAuditLogExport)
	mux.HandleFunc("/api/v1/doris/explain", server.handleDorisExplain)
	mux.HandleFunc("/api/v1/doris/explain/tree", server.handleDorisExplain)
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

func (s *Server) handleDorisConnectionTest(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req struct {
		Connection *dorisConnection `json:"connection"`
	}
	if !readJSONOrWriteError(w, r, &req) {
		return
	}
	cfg, ok := parseConnConfigOrWriteError(w, r, req.Connection)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	applyReadWriteTimeout(&cfg, 15*time.Second)
	version, err := s.queryVersion(ctx, cfg)
	if err != nil {
		writeErrorWithRequest(w, r, http.StatusBadRequest, err.Error())
		return
	}
	writeData(w, r, http.StatusOK, map[string]any{
		"version": version,
	})
}

func (s *Server) handleDorisDatabases(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req struct {
		Connection *dorisConnection `json:"connection"`
	}
	if !readJSONOrWriteError(w, r, &req) {
		return
	}
	cfg, ok := parseConnConfigOrWriteError(w, r, req.Connection)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	applyReadWriteTimeout(&cfg, 20*time.Second)
	databases, err := s.listDatabases(ctx, cfg)
	if err != nil {
		writeErrorWithRequest(w, r, http.StatusBadRequest, err.Error())
		return
	}
	writeData(w, r, http.StatusOK, map[string]any{
		"databases": databases,
	})
}

func (s *Server) handleDorisAuditLogExport(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req struct {
		Connection      *dorisConnection `json:"connection"`
		LookbackSeconds int              `json:"lookbackSeconds"`
		Limit           int              `json:"limit"`
	}
	if !readJSONOrWriteError(w, r, &req) {
		return
	}
	cfg, ok := parseConnConfigOrWriteError(w, r, req.Connection)
	if !ok {
		return
	}
	if req.LookbackSeconds <= 0 {
		writeErrorWithRequest(w, r, http.StatusBadRequest, "lookbackSeconds must be positive")
		return
	}
	if req.Limit <= 0 {
		writeErrorWithRequest(w, r, http.StatusBadRequest, "limit must be positive")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.exportTimeout)
	defer cancel()

	w.Header().Set("Content-Type", "text/tab-separated-values; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="audit_log.tsv"`)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	cw := &countingWriter{w: w}
	applyReadWriteTimeout(&cfg, s.exportTimeout+10*time.Second)
	if err := s.exportAuditLog(ctx, cfg, req.LookbackSeconds, req.Limit, cw); err != nil {
		if cw.n == 0 {
			w.Header().Del("Content-Disposition")
			writeErrorWithRequest(w, r, http.StatusBadRequest, err.Error())
			return
		}
		// Avoid silently importing a truncated TSV.
		panic(http.ErrAbortHandler)
	}
}

func (s *Server) handleDorisExplain(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req struct {
		Connection *dorisConnection `json:"connection"`
		SQL        string           `json:"sql"`
		Mode       string           `json:"mode"`
	}
	if !readJSONOrWriteError(w, r, &req) {
		return
	}
	cfg, ok := parseConnConfigOrWriteError(w, r, req.Connection)
	if !ok {
		return
	}
	sqlText := strings.TrimSpace(req.SQL)
	if sqlText == "" {
		writeErrorWithRequest(w, r, http.StatusBadRequest, "sql is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	applyReadWriteTimeout(&cfg, 20*time.Second)
	mode, err := normalizeExplainMode(req.Mode)
	if err != nil {
		writeErrorWithRequest(w, r, http.StatusBadRequest, err.Error())
		return
	}

	rawText, err := s.explain(ctx, cfg, sqlText, mode)
	if err != nil {
		writeErrorWithRequest(w, r, http.StatusBadRequest, err.Error())
		return
	}
	writeData(w, r, http.StatusOK, map[string]any{
		"rawText": rawText,
	})
}
