package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/rbwsam/ferry/util"
)

type TableCopier struct {
	Name        string
	Source      *DB
	Destination *DB
}

func NewTableCopier(tableName string, srcCfg, destCfg *Config) *TableCopier {
	return &TableCopier{
		Name:        tableName,
		Source:      NewDB(srcCfg),
		Destination: NewDB(destCfg),
	}
}

func (tc *TableCopier) Copy() {
	log.Printf("Starting to copy table `%s`", tc.Name)

	rows := tc.getRows()
	defer rows.Close()

	tc.Destination.LockTable(tc.Name)
	tc.Destination.DisableKeys(tc.Name)

	var buffer bytes.Buffer
	for rows.Next() {
		values := tc.scanToValues(rows)

		if buffer.Len()+len(values) > 1e+6 { // 1MB
			tc.insertValues(&buffer)
			buffer = bytes.Buffer{}
		}
		buffer.WriteString(values)
		buffer.WriteString(",")
	}
	if buffer.Len() > 0 {
		tc.insertValues(&buffer)
	}

	tc.Destination.EnableKeys(tc.Name)
	tc.Destination.UnlockTables()

	util.CheckError(rows.Err())
	log.Printf("Done copying table `%s`", tc.Name)
}

func (tc *TableCopier) Close() {
	tc.Source.Close()
	tc.Destination.Close()
}

func (tc *TableCopier) insertValues(b *bytes.Buffer) {
	trimmed := strings.TrimRight(b.String(), ",")
	insertQuery := fmt.Sprintf("INSERT INTO `%s` VALUES %s", tc.Name, trimmed)
	tc.Destination.Exec(insertQuery)
}

func (tc *TableCopier) scanToValues(rows *sql.Rows) string {
	rowSlice := tc.scanSlice(rows)
	return tc.scanSliceToString(rowSlice)
}

func (tc *TableCopier) CreateTable() {
	query := tc.Source.CreateTableQuery(tc.Name)
	tc.Destination.Exec(query)
}

func (tc *TableCopier) getRows() *sql.Rows {
	query := fmt.Sprintf("SELECT * FROM %s", tc.Name)
	return tc.Source.Query(query)
}

func (tc *TableCopier) getColumns(rows *sql.Rows) []string {
	columns, err := rows.Columns()
	util.CheckError(err)
	return columns
}

func (tc *TableCopier) scanSlice(rows *sql.Rows) []sql.NullString {
	columns := tc.getColumns(rows)
	values, pointers := RowSlice(len(columns))
	scanErr := rows.Scan(pointers...)
	util.CheckError(scanErr)
	return values
}

func (tc *TableCopier) scanSliceToString(rowSlice []sql.NullString) string {
	tmpSlice := []string{}
	for _, value := range rowSlice {
		if value.Valid {
			escaped := EscapeString(value.String)
			wrapped := fmt.Sprintf("'%s'", escaped)
			tmpSlice = append(tmpSlice, wrapped)
		} else {
			tmpSlice = append(tmpSlice, "NULL")
		}
	}
	res := strings.Join(tmpSlice, ",")
	return fmt.Sprintf("(%s)", res)
}
