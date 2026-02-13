package api

import (
	"crypto/rand"
	"encoding/json"
	"encoding/hex"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"
)

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeData(w http.ResponseWriter, r *http.Request, status int, data any) {
	traceID := resolveTraceID(r)
	writeEnvelope(w, status, traceID, map[string]any{
		"ok":      true,
		"data":    data,
		"traceId": traceID,
	})
}

func writeErrorWithRequest(w http.ResponseWriter, r *http.Request, status int, message string) {
	traceID := resolveTraceID(r)
	writeEnvelope(w, status, traceID, map[string]any{
		"ok":      false,
		"error":   map[string]any{"message": message},
		"traceId": traceID,
	})
}

func writeEnvelope(w http.ResponseWriter, status int, traceID string, body map[string]any) {
	w.Header().Set("X-Trace-Id", traceID)
	writeJSON(w, status, body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeErrorWithRequest(w, nil, status, message)
}

func resolveTraceID(r *http.Request) string {
	if r != nil {
		for _, key := range []string{"X-Trace-Id", "X-Request-Id"} {
			v := strings.TrimSpace(r.Header.Get(key))
			if v != "" {
				return v
			}
		}
	}
	return generateTraceID()
}

func generateTraceID() string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return hex.EncodeToString(buf[:])
	}
	return strings.ReplaceAll(time.Now().UTC().Format("20060102T150405.000000000"), ".", "")
}

func readJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mt != "application/json" {
		return errors.New("Content-Type must be application/json")
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(&struct{}{}); err == nil {
		return errors.New("unexpected trailing JSON")
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}
