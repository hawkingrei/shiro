package runner

import (
	"errors"
	"strings"

	"shiro/internal/util"

	"github.com/go-sql-driver/mysql"
)

func isPanicError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "panic") || strings.Contains(msg, "assert") || strings.Contains(msg, "internal error")
}

// sqlErrorWhitelist lists MySQL error codes considered fuzz-tool faults.
// 1064 is the generic SQL syntax error, common for malformed generated SQL.
// 1292 is a type truncation error triggered by type-mismatched predicates.
// 1451 is a foreign key constraint failure when deleting/updating parent rows.
// 1452 is a foreign key constraint failure during child insert/update.
var sqlErrorWhitelist = map[uint16]struct{}{
	1064: {},
	1292: {},
	1451: {},
	1452: {},
}

func isWhitelistedSQLError(err error) (uint16, bool) {
	if err == nil {
		return 0, false
	}
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		_, ok := sqlErrorWhitelist[mysqlErr.Number]
		return mysqlErr.Number, ok
	}
	return 0, false
}

func logWhitelistedSQLError(sqlText string, err error, verbose bool) bool {
	code, ok := isWhitelistedSQLError(err)
	if !ok {
		return false
	}
	if verbose {
		util.Detailf("sql error whitelisted code=%d sql=%s err=%v", code, sqlText, err)
	}
	return true
}

func isMySQLError(err error) bool {
	if err == nil {
		return false
	}
	var mysqlErr *mysql.MySQLError
	return errors.As(err, &mysqlErr)
}
