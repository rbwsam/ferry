package mysql

import (
	"database/sql"
	"strings"
)

const DriverName = "mysql"

func RowSlice(length int) ([]sql.NullString, []interface{}) {
	values := make([]sql.NullString, length)
	pointers := make([]interface{}, length)
	for i := range values {
		pointers[i] = &values[i]
	}
	return values, pointers
}

func EscapeString(s string) string {
	s = strings.Replace(s, "\x00", "\\x00", -1)
	s = strings.Replace(s, "\x1a", "\\x1a", -1)
	s = strings.Replace(s, "\n", "\\n", -1)
	s = strings.Replace(s, "\r", "\\r", -1)
	s = strings.Replace(s, "\\", "\\\\", -1)
	s = strings.Replace(s, "'", "\\'", -1)
	s = strings.Replace(s, "\"", "\\\"", -1)
	return s
}
