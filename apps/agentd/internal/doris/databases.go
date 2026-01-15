package doris

import (
	"context"
	"database/sql"
	"errors"
	"sort"
)

func ListDatabases(ctx context.Context, cfg ConnConfig) ([]string, error) {
	cfg.Database = ""
	db, err := openAndPing(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if len(cols) < 1 {
		return nil, errors.New("unexpected SHOW DATABASES result: no columns")
	}

	databases := make([]string, 0, 64)
	for rows.Next() {
		var name sql.NullString
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name.Valid && name.String != "" {
			databases = append(databases, name.String)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	sort.Strings(databases)
	return databases, nil
}
