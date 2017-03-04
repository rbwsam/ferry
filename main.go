package main

import (
	"github.com/rbwsam/ferry/mysql"
	"github.com/rbwsam/ferry/ferry"
	"io/ioutil"
	"encoding/json"
	"github.com/rbwsam/ferry/util"
	"flag"
)

func main() {
	configPath := flag.String("config", "ferry.json", "Path to JSON config file")
	flag.Parse()

	srcCfg, destCfg := getConfigs(configPath)
	prepareDest(destCfg)
	tables := getTables(srcCfg)

	for _, tableName := range *tables {
		if tableName == "sessions" || tableName == "versions" {
			continue
		}
		copier := mysql.NewTableCopier(tableName, srcCfg, destCfg)
		copier.Copy()
		copier.Close()
	}
}

func getConfigs(path *string) (*mysql.Config, *mysql.Config) {
	var config ferry.Config
	file, err := ioutil.ReadFile(*path)
	util.CheckError(err)
	json.Unmarshal(file, &config)
	return &config.Source, &config.Destination
}

func prepareDest(c *mysql.Config) {
	db := mysql.NewDB(c)
	db.Drop()
	db.Create()
	db.Close()
}

func getTables(c *mysql.Config) *[]string {
	db := mysql.NewDB(c)
	return db.ListTables()
}
