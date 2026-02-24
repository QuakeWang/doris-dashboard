package api

import (
	"context"
	"net/http"
	"strings"
	"time"
)

type connectionRequest struct {
	Connection *dorisConnection `json:"connection"`
}

type auditExportRequest struct {
	Connection      *dorisConnection `json:"connection"`
	LookbackSeconds int              `json:"lookbackSeconds"`
	Limit           int              `json:"limit"`
}

type explainRequest struct {
	Connection *dorisConnection `json:"connection"`
	SQL        string           `json:"sql"`
	Mode       string           `json:"mode"`
}

func (s *Server) handleDorisConnectionTest(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req connectionRequest
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
	var req connectionRequest
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
	var req auditExportRequest
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
	var req explainRequest
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
