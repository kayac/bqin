package bqin

import "log"

func debugf(format string, args ...interface{}) {
	log.Printf("[debug] "+format, args...)
}

func infof(format string, args ...interface{}) {
	log.Printf("[info] "+format, args...)
}

func errorf(format string, args ...interface{}) {
	log.Printf("[error] "+format, args...)
}
