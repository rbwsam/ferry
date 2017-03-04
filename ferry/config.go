package ferry

import "github.com/rbwsam/ferry/mysql"

type Config struct {
	Source      mysql.Config `json:"source"`
	Destination mysql.Config `json:"destination"`
}
