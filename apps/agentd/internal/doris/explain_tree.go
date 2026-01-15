package doris

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

const (
	explainSQLMaxBytes    = 256_000
	explainOutputMaxBytes = 4_000_000
	asciiWhitespace       = " \t\n\r\f\v"
)

func parseLeadingUseDatabase(sqlText string) (db string, rest string, ok bool, err error) {
	trimmed := strings.TrimSpace(sqlText)
	if trimmed == "" {
		return "", "", false, nil
	}

	word, _ := scanLeadingWord(trimmed)
	if strings.ToUpper(word) != "USE" {
		return "", sqlText, false, nil
	}
	afterUse := strings.TrimSpace(trimmed[len(word):])
	if afterUse == "" {
		return "", "", true, errors.New("USE statement requires a database name")
	}

	var dbName string
	var tail string
	if afterUse[0] == '`' {
		end := strings.Index(afterUse[1:], "`")
		if end < 0 {
			return "", "", true, errors.New("USE statement has an unterminated quoted identifier")
		}
		dbName = afterUse[1 : 1+end]
		tail = afterUse[1+end+1:]
	} else {
		i := 0
		for i < len(afterUse) {
			c := afterUse[i]
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
				i++
				continue
			}
			break
		}
		if i == 0 {
			return "", "", true, errors.New("USE statement has an invalid database name")
		}
		if i < len(afterUse) {
			c := afterUse[i]
			if c != ';' && !strings.ContainsRune(asciiWhitespace, rune(c)) {
				return "", "", true, errors.New("USE statement has an invalid database name")
			}
		}
		dbName = afterUse[:i]
		tail = afterUse[i:]
	}
	dbName = strings.TrimSpace(dbName)
	if dbName == "" {
		return "", "", true, errors.New("USE statement requires a database name")
	}

	tail = strings.TrimLeft(tail, asciiWhitespace)
	if tail == "" || tail[0] != ';' {
		return "", "", true, errors.New("USE statement must end with ';'")
	}
	restSQL := strings.TrimSpace(tail[1:])
	if restSQL == "" {
		return "", "", true, errors.New("sql is required after USE")
	}
	return dbName, restSQL, true, nil
}

var explainPlanTypeTokens = map[string]struct{}{
	"PARSED":      {},
	"ANALYZED":    {},
	"REWRITTEN":   {},
	"LOGICAL":     {},
	"OPTIMIZED":   {},
	"PHYSICAL":    {},
	"SHAPE":       {},
	"MEMO":        {},
	"DISTRIBUTED": {},
	"ALL":         {},
}

var explainLevelTokens = map[string]struct{}{
	"VERBOSE": {},
	"TREE":    {},
	"GRAPH":   {},
	"PLAN":    {},
	"DUMP":    {},
}

func scanLeadingWord(s string) (word string, rest string) {
	s = strings.TrimLeft(s, asciiWhitespace)
	if s == "" {
		return "", ""
	}
	i := 0
	for i < len(s) {
		c := s[i]
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' {
			i++
			continue
		}
		break
	}
	if i == 0 {
		return "", s
	}
	return s[:i], s[i:]
}

func stripLeadingCommentsAndSpace(s string) string {
	for {
		s = strings.TrimLeft(s, asciiWhitespace)
		if s == "" {
			return ""
		}
		if strings.HasPrefix(s, "--") {
			nl := strings.IndexByte(s, '\n')
			if nl < 0 {
				return ""
			}
			s = s[nl+1:]
			continue
		}
		if strings.HasPrefix(s, "/*") {
			end := strings.Index(s, "*/")
			if end < 0 {
				return ""
			}
			s = s[end+2:]
			continue
		}
		return s
	}
}

func buildExplainTreeQuery(sqlText string) (string, error) {
	sqlText = strings.TrimSpace(sqlText)
	if sqlText == "" {
		return "", errors.New("sql is required")
	}
	if len(sqlText) > explainSQLMaxBytes {
		return "", fmt.Errorf("sql too large: %d bytes (max=%d)", len(sqlText), explainSQLMaxBytes)
	}
	sqlText = strings.TrimRight(sqlText, ";")
	if strings.TrimSpace(sqlText) == "" {
		return "", errors.New("sql is required")
	}

	upper := strings.ToUpper(sqlText)
	if strings.HasPrefix(upper, "EXPLAIN") {
		rest := strings.TrimSpace(sqlText[len("EXPLAIN"):])
		if rest == "" {
			return "", errors.New("sql is required")
		}

		planType := ""
		level := ""
		process := false

		word, remain := scanLeadingWord(rest)
		wordUpper := strings.ToUpper(word)
		if _, ok := explainPlanTypeTokens[wordUpper]; ok {
			planType = wordUpper
			rest = strings.TrimSpace(remain)
			word, remain = scanLeadingWord(rest)
			wordUpper = strings.ToUpper(word)
		}

		if _, ok := explainLevelTokens[wordUpper]; ok {
			level = wordUpper
			rest = strings.TrimSpace(remain)
			word, remain = scanLeadingWord(rest)
			wordUpper = strings.ToUpper(word)
		}

		if wordUpper == "PROCESS" {
			process = true
			rest = strings.TrimSpace(remain)
		}
		if process {
			return "", errors.New("EXPLAIN PROCESS is not supported")
		}
		if level != "" && level != "TREE" {
			return "", errors.New("only EXPLAIN TREE is supported")
		}

		check := strings.ToUpper(stripLeadingCommentsAndSpace(rest))
		if strings.HasPrefix(check, "SELECT") ||
			strings.HasPrefix(check, "WITH") ||
			strings.HasPrefix(check, "INSERT") ||
			strings.HasPrefix(check, "UPDATE") ||
			strings.HasPrefix(check, "DELETE") {
			if planType != "" {
				return "EXPLAIN " + planType + " TREE " + rest, nil
			}
			return "EXPLAIN TREE " + rest, nil
		}
		return "", errors.New("only EXPLAIN TREE is supported")
	}

	return "EXPLAIN TREE " + sqlText, nil
}

func ExplainTree(ctx context.Context, cfg ConnConfig, sqlText string) (string, error) {
	dbName, restSQL, hasUse, err := parseLeadingUseDatabase(sqlText)
	if err != nil {
		return "", err
	}
	if hasUse {
		sqlText = restSQL
		cfg.Database = ""
	}

	queryText, err := buildExplainTreeQuery(sqlText)
	if err != nil {
		return "", err
	}

	db, err := openAndPing(ctx, cfg)
	if err != nil {
		return "", err
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	if hasUse {
		if strings.Contains(dbName, "`") {
			return "", errors.New("USE database name contains invalid character: '`'")
		}
		if _, err := conn.ExecContext(ctx, "USE `"+dbName+"`"); err != nil {
			return "", err
		}
	}

	rows, err := conn.QueryContext(ctx, queryText)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if len(cols) < 1 {
		return "", errors.New("unexpected explain result: no columns")
	}

	var (
		b        strings.Builder
		line     sql.NullString
		discard  any
		scanDest = make([]any, len(cols))
	)
	scanDest[0] = &line
	for i := 1; i < len(scanDest); i++ {
		scanDest[i] = &discard
	}
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		if err := rows.Scan(scanDest...); err != nil {
			return "", err
		}
		if line.Valid {
			b.WriteString(line.String)
		}
		b.WriteString("\n")
		if b.Len() > explainOutputMaxBytes {
			return "", fmt.Errorf("explain output too large: %d bytes (max=%d)", b.Len(), explainOutputMaxBytes)
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return strings.TrimRight(b.String(), "\n"), nil
}
