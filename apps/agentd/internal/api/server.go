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
	return doris.ConnConfig{
		Host:     host,
		Port:     c.Port,
		User:     user,
		Password: c.Password,
	}, nil
}

type Server struct {
	exportAuditLog AuditLogExporter
	queryVersion   func(ctx context.Context, cfg doris.ConnConfig) (string, error)
	exportTimeout  time.Duration
}

func NewServer(
	exporter AuditLogExporter,
	exportTimeout time.Duration,
	queryVersion ...func(ctx context.Context, cfg doris.ConnConfig) (string, error),
) http.Handler {
	if exporter == nil {
		exporter = doris.StreamAuditLogOutfileTSVLookback
	}
	if exportTimeout <= 0 {
		exportTimeout = 60 * time.Second
	}

	qv := doris.QueryVersion
	if len(queryVersion) > 0 && queryVersion[0] != nil {
		qv = queryVersion[0]
	}
	server := &Server{
		exportAuditLog: exporter,
		queryVersion:   qv,
		exportTimeout:  exportTimeout,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", server.handleHealth)
	mux.HandleFunc("/api/v1/doris/connection/test", server.handleDorisConnectionTest)
	mux.HandleFunc("/api/v1/doris/audit-log/export", server.handleDorisAuditLogExport)
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
			writeError(w, http.StatusForbidden, "loopback only")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && !isAllowedOrigin(origin) {
			writeError(w, http.StatusForbidden, "origin not allowed")
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
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleDorisConnectionTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Connection *dorisConnection `json:"connection"`
	}
	if err := readJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	cfg, err := parseConnConfig(req.Connection)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	cfg.ReadTimeout = 15 * time.Second
	cfg.WriteTimeout = 15 * time.Second
	version, err := s.queryVersion(ctx, cfg)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"version": version,
	})
}

func (s *Server) handleDorisAuditLogExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	var req struct {
		Connection      *dorisConnection `json:"connection"`
		LookbackSeconds int              `json:"lookbackSeconds"`
		Limit           int              `json:"limit"`
	}
	if err := readJSON(w, r, &req); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	cfg, err := parseConnConfig(req.Connection)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.LookbackSeconds <= 0 {
		writeError(w, http.StatusBadRequest, "lookbackSeconds must be positive")
		return
	}
	if req.Limit <= 0 {
		writeError(w, http.StatusBadRequest, "limit must be positive")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.exportTimeout)
	defer cancel()

	w.Header().Set("Content-Type", "text/tab-separated-values; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="audit_log.tsv"`)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	cw := &countingWriter{w: w}
	cfg.ReadTimeout = s.exportTimeout + 10*time.Second
	cfg.WriteTimeout = s.exportTimeout + 10*time.Second
	if err := s.exportAuditLog(ctx, cfg, req.LookbackSeconds, req.Limit, cw); err != nil {
		if cw.n == 0 {
			w.Header().Del("Content-Disposition")
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		// Avoid silently importing a truncated TSV.
		panic(http.ErrAbortHandler)
	}
}
