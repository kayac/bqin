package bqin_test

import (
	"net/url"
	"os"

	"github.com/kayac/bqin/internal/logger"
)

func GetTestLogLevel() string {
	debug := os.Getenv("DEBUG")
	if debug == "" {
		return logger.InfoLevel
	}
	return logger.DebugLevel
}

func MustParseURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return u
}
