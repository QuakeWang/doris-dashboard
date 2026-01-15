package doris

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
)

const (
	auditLogOutfileCols  = 29
	auditLogMaxLimit     = 200_000
	auditLogDefaultLimit = 50_000

	auditLogMaxLookbackSeconds     = 30 * 24 * 3600
	auditLogDefaultLookbackSeconds = 3600
)

func StreamAuditLogOutfileTSVLookback(
	ctx context.Context,
	cfg ConnConfig,
	lookbackSeconds int,
	limit int,
	w io.Writer,
) error {
	if lookbackSeconds <= 0 {
		lookbackSeconds = auditLogDefaultLookbackSeconds
	}
	if lookbackSeconds > auditLogMaxLookbackSeconds {
		return fmt.Errorf(
			"lookbackSeconds too large: %d (max=%d)",
			lookbackSeconds,
			auditLogMaxLookbackSeconds,
		)
	}
	if limit <= 0 {
		limit = auditLogDefaultLimit
	}
	if limit > auditLogMaxLimit {
		return fmt.Errorf("limit too large: %d (max=%d)", limit, auditLogMaxLimit)
	}

	db, err := openAndPing(ctx, cfg)
	if err != nil {
		return err
	}
	defer db.Close()

	q := fmt.Sprintf(
		"SELECT * FROM `__internal_schema`.`audit_log` "+
			"WHERE `time` >= DATE_SUB(NOW(), INTERVAL %d SECOND) AND `time` <= NOW() "+
			"ORDER BY `time` DESC LIMIT %d",
		lookbackSeconds,
		limit,
	)
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(cols) < auditLogOutfileCols {
		return fmt.Errorf(
			"unexpected audit_log columns: %d (expected >= %d)",
			len(cols),
			auditLogOutfileCols,
		)
	}
	outCols := cols[:auditLogOutfileCols]
	checks := []struct {
		idx   int
		names []string
	}{
		{0, []string{"query_id"}},
		{1, []string{"time"}},
		{2, []string{"client_ip"}},
		{3, []string{"user", "user_name"}},
		{5, []string{"db", "db_name"}},
		{6, []string{"state"}},
		{7, []string{"error_code"}},
		{8, []string{"error_message"}},
		{9, []string{"time(ms)", "time_ms", "query_time", "query_time_ms"}},
		{10, []string{"scan_bytes"}},
		{11, []string{"scan_rows"}},
		{12, []string{"return_rows"}},
		{21, []string{"fe_ip", "frontend_ip"}},
		{22, []string{"cpu_time_ms"}},
		{25, []string{"peak_memory_bytes"}},
		{26, []string{"workload_group"}},
		{27, []string{"cloud_cluster_name", "compute_group_name", "compute_group"}},
		{28, []string{"stmt"}},
	}
	for _, c := range checks {
		got := strings.ToLower(outCols[c.idx])
		if slices.Contains(c.names, got) {
			continue
		}
		return fmt.Errorf(
			"unexpected audit_log column[%d]: %q (expected %s)",
			c.idx,
			outCols[c.idx],
			strings.Join(c.names, " or "),
		)
	}

	raw := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range raw {
		ptrs[i] = &raw[i]
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return errors.New("no audit_log rows found in the selected lookback window")
	}

	bw := bufio.NewWriterSize(w, 256*1024)
	if _, err := bw.WriteString(strings.Join(outCols, "\t") + "\n"); err != nil {
		return err
	}

	row := make([]string, auditLogOutfileCols)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}
		for i := 0; i < auditLogOutfileCols; i++ {
			row[i] = formatOutfileField(raw[i])
		}
		if _, err := bw.WriteString(strings.Join(row, "\t") + "\n"); err != nil {
			return err
		}
		if !rows.Next() {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return bw.Flush()
}

func formatOutfileField(v any) string {
	if v == nil {
		return `\N`
	}
	var s string
	switch x := v.(type) {
	case []byte:
		s = string(x)
	case time.Time:
		s = x.Format("2006-01-02 15:04:05.000000")
	default:
		s = fmt.Sprint(x)
	}
	if s == "" {
		return ""
	}
	return escapeOutfileText(s)
}

func escapeOutfileText(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}
