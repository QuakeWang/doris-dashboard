package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/QuakeWang/doris-dashboard/apps/agentd/internal/doris"
	"github.com/go-sql-driver/mysql"
)

type schemaAuditScanRequest struct {
	Connection *dorisConnection `json:"connection"`
	Database   string           `json:"database"`
	TableLike  string           `json:"tableLike"`
	Page       int              `json:"page"`
	PageSize   int              `json:"pageSize"`
}

type schemaAuditTableDetailRequest struct {
	Connection *dorisConnection `json:"connection"`
	Database   string           `json:"database"`
	Table      string           `json:"table"`
}

func (s *Server) handleDorisSchemaAuditScan(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	var req schemaAuditScanRequest
	if !readJSONOrWriteError(w, r, &req) {
		return
	}
	cfg, ok := parseConnConfigOrWriteError(w, r, req.Connection)
	if !ok {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	applyReadWriteTimeout(&cfg, 70*time.Second)

	result, err := s.schemaAuditScan(ctx, cfg, doris.SchemaAuditScanOptions{
		Database:  strings.TrimSpace(req.Database),
		TableLike: strings.TrimSpace(req.TableLike),
		Page:      req.Page,
		PageSize:  req.PageSize,
	})
	if err != nil {
		writeErrorWithRequest(w, r, schemaAuditStatusCode(err), err.Error())
		return
	}
	writeData(w, r, http.StatusOK, result)
}

func (s *Server) handleDorisSchemaAuditTableDetail(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}

	var req schemaAuditTableDetailRequest
	if !readJSONOrWriteError(w, r, &req) {
		return
	}
	cfg, ok := parseConnConfigOrWriteError(w, r, req.Connection)
	if !ok {
		return
	}

	database := strings.TrimSpace(req.Database)
	if database == "" {
		writeErrorWithRequest(w, r, http.StatusBadRequest, "database is required")
		return
	}
	table := strings.TrimSpace(req.Table)
	if table == "" {
		writeErrorWithRequest(w, r, http.StatusBadRequest, "table is required")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	applyReadWriteTimeout(&cfg, 25*time.Second)
	result, err := s.schemaAuditTableDetail(ctx, cfg, database, table)
	if err != nil {
		writeErrorWithRequest(w, r, schemaAuditStatusCode(err), err.Error())
		return
	}
	writeData(w, r, http.StatusOK, result)
}

func schemaAuditStatusCode(err error) int {
	if isSchemaAuditRequestError(err) {
		return http.StatusBadRequest
	}
	// Schema audit handlers are a proxy to Doris metadata queries.
	// Non-validation failures are treated as upstream dependency failures.
	return http.StatusBadGateway
}

func isSchemaAuditRequestError(err error) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		if isSchemaAuditRequestMySQLError(mysqlErr.Number, mysqlErr.Message) {
			return true
		}
	}

	message := strings.ToLower(strings.TrimSpace(err.Error()))
	if message == "" {
		return false
	}
	return strings.HasSuffix(message, "is required") ||
		strings.HasSuffix(message, "is invalid") ||
		strings.Contains(message, "filter is invalid")
}

func isSchemaAuditRequestMySQLError(number uint16, message string) bool {
	switch number {
	case 1049: // ER_BAD_DB_ERROR
		return true
	case 1109: // ER_UNKNOWN_TABLE
		return true
	case 1146: // ER_NO_SUCH_TABLE
		return true
	case 1105: // ER_UNKNOWN_ERROR (Doris may wrap unknown table/database in detailMessage)
		return isSchemaAuditUnknownObjectMessage(message)
	default:
		return false
	}
}

func isSchemaAuditUnknownObjectMessage(message string) bool {
	normalized := strings.ToLower(strings.TrimSpace(message))
	if normalized == "" {
		return false
	}
	return strings.Contains(normalized, "unknown database") ||
		strings.Contains(normalized, "unknown table") ||
		strings.Contains(normalized, "doesn't exist") ||
		strings.Contains(normalized, "does not exist")
}
