package config

import (
	"log"
	"os"
	"regexp"
)

func GetEnv(key string, defaultValue string) string {
	v, ok := os.LookupEnv(key)
	if !ok {
		log.Printf("[DEBUG] get env <%s>: <%s> (default)", key, defaultValue)
		return defaultValue
	}
	log.Printf("[DEBUG] get env <%s>: <%s>", key, v)
	return v
}

func GetSecret(key string, defaultValue string) string {
	v, ok := os.LookupEnv(key)
	re := regexp.MustCompile(`(^.*$)`)

	if !ok {
		log.Printf("[DEBUG] get secret env <%s>: <%s> (default)", key, re.ReplaceAllString(defaultValue, `******`))
		return defaultValue
	}
	log.Printf("[DEBUG] get secret env <%s>: <%s>", key, re.ReplaceAllString(v, `******`))
	return v
}
