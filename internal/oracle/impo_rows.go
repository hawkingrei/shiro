package oracle

import (
	"context"
	"database/sql"
	"strings"

	"shiro/internal/db"
	"shiro/internal/util"
)

type rowSet struct {
	columns int
	rows    []string
}

func queryRowSet(ctx context.Context, exec *db.DB, query string, maxRows int) (rowSet, bool, error) {
	rows, err := exec.QueryContext(ctx, query)
	if err != nil {
		return rowSet{}, false, err
	}
	defer util.CloseWithErr(rows, "impo rows")

	cols, err := rows.Columns()
	if err != nil {
		return rowSet{}, false, err
	}
	if maxRows <= 0 {
		maxRows = 50
	}
	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	out := rowSet{columns: len(cols), rows: make([]string, 0)}
	truncated := false
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return rowSet{}, false, err
		}
		if len(out.rows) < maxRows {
			parts := make([]string, 0, len(values))
			for _, v := range values {
				if v == nil {
					parts = append(parts, "NULL")
					continue
				}
				parts = append(parts, string(v))
			}
			out.rows = append(out.rows, strings.Join(parts, "\x1f"))
		} else {
			truncated = true
		}
	}
	return out, truncated, rows.Err()
}

func compareRowSets(base rowSet, other rowSet) (int, error) {
	empty1 := base.columns == 0
	empty2 := other.columns == 0
	if empty1 || empty2 {
		switch {
		case empty1 && empty2:
			return 0, nil
		case empty1:
			return -1, nil
		default:
			return 1, nil
		}
	}
	if base.columns != other.columns {
		return 2, nil
	}

	mp := make(map[string]int, len(other.rows))
	for _, row := range other.rows {
		mp[row]++
	}
	allInOther := true
	for _, row := range base.rows {
		if num, ok := mp[row]; ok {
			if num <= 1 {
				delete(mp, row)
			} else {
				mp[row] = num - 1
			}
		} else {
			allInOther = false
		}
	}

	if allInOther {
		if len(mp) == 0 {
			return 0, nil
		}
		return -1, nil
	}
	if len(mp) == 0 {
		return 1, nil
	}
	return 2, nil
}
