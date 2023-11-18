package evently

import (
	"log"
	"os"
)

func DEBUG(format string, args ...interface{}) {
	if os.Getenv("DEBUG") != "" {
		log.Printf(format, args...)
	}
}
