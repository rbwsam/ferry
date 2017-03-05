# ferry

MySQL database copy tool written in Golang

## Run

```bash
$ cd $GOPATH
$ git clone git@github.com:rbwsam/ferry.git
$ cd ferry
$ go get -u github.com/kardianos/govendor
$ $GOPATH/bin/govendor sync
$ go run main.go --config /path/to/config.json
```
