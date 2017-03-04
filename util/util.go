package util

import (
	"log"
)

func CheckError(error error) {
	if error != nil {
		log.Panic(error)
	}
}
