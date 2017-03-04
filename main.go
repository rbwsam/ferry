package main

import (
	"github.com/rbwsam/ferry/mysql"
	"runtime"
	"sync"
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

	var wg sync.WaitGroup
	jobs := make(chan string)

	worker := func(jobs <-chan string) {
		for tableName := range jobs {
			copier := mysql.NewTableCopier(tableName, srcCfg, destCfg)
			if tableName == "sessions" || tableName == "versions" {
				copier.CreateTable()
			} else {
				copier.Copy()
			}
			copier.Close()
		}
		wg.Done()
	}

	for w := 0; w < runtime.GOMAXPROCS(-1); w++ {
		wg.Add(1)
		go worker(jobs)
	}

	for _, name := range *tables {
		jobs <- name
	}
	close(jobs)
	wg.Wait()
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
