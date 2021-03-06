package logger

import (
	"encoding/json"
	"io"
	"log"

	"github.com/hashicorp/logutils"
)

const (
	DebugLevel = "debug"
	InfoLevel  = "info"
	ErrorLevel = "error"
)

func Setup(out io.Writer, minLevel string) {
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{DebugLevel, InfoLevel, ErrorLevel},
		MinLevel: logutils.LogLevel(minLevel),
		Writer:   out,
	}
	log.SetOutput(filter)
}

func Debugf(format string, args ...interface{}) {
	printf(DebugLevel, format, args...)
}

func Infof(format string, args ...interface{}) {
	printf(InfoLevel, format, args...)
}

func Errorf(format string, args ...interface{}) {
	printf(ErrorLevel, format, args...)
}

func printf(level, format string, args ...interface{}) {
	log.Printf("["+level+"] "+format, args...)
}

func InfoDump(v interface{}) {
	dump(InfoLevel, v)
}

func dump(level string, v interface{}) {
	s, err := json.Marshal(v)
	if err != nil {
		printf(level, "%+v", v)
		return
	}
	printf(level, string(s))
}
