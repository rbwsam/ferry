package mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/rbwsam/ferry/util"
)

type DB struct {
	config *Config
	sqlDB  *sql.DB
}

func NewDB(c *Config) *DB {
	db, err := sql.Open(DriverName, c.DSN())
	util.CheckError(err)
	return &DB{
		config: c,
		sqlDB:  db,
	}
}

func (db *DB) Create() {
	query := fmt.Sprintf("CREATE DATABASE `%s`", db.config.Database)
	db.Exec(query)
}

func (db *DB) Drop() {
	query := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", db.config.Database)
	db.Exec(query)
}

func (db *DB) Exec(query string, args ...interface{}) sql.Result {
	res, err := db.sqlDB.Exec(query, args...)
	util.CheckError(err)
	return res
}

func (db *DB) Query(query string, args ...interface{}) *sql.Rows {
	rows, err := db.sqlDB.Query(query, args...)
	util.CheckError(err)
	return rows
}

func (db *DB) Close() {
	db.sqlDB.Close()
}

func (db *DB) ListTables() *[]string {
	rows := db.Query("SHOW TABLES")
	defer rows.Close()

	tables := []string{}

	for rows.Next() {
		var name string
		scanErr := rows.Scan(&name)
		util.CheckError(scanErr)
		tables = append(tables, name)
	}
	util.CheckError(rows.Err())
	return &tables
}

func (db *DB) DisableKeys(tableName string) {
	query := fmt.Sprintf("ALTER TABLE `%s` DISABLE KEYS", tableName)
	db.Exec(query)
}

func (db *DB) EnableKeys(tableName string) {
	query := fmt.Sprintf("ALTER TABLE `%s` ENABLE KEYS", tableName)
	db.Exec(query)
}

func (db *DB) LockTable(tableName string) {
	query := fmt.Sprintf("LOCK TABLES `%s` WRITE", tableName)
	db.Exec(query)
}

func (db *DB) UnlockTables() {
	db.Exec("UNLOCK TABLES")
}

func (db *DB) CreateTableQuery(name string) string {
	query := fmt.Sprintf("SHOW CREATE TABLE %s", name)
	rows := db.Query(query)
	defer rows.Close()
	var unused, result string
	for rows.Next() {
		scanErr := rows.Scan(&unused, &result)
		util.CheckError(scanErr)
	}
	util.CheckError(rows.Err())
	return result
}
