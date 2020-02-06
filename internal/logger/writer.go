package logger

import "testing"

type TestingLogWriter struct {
	t *testing.T
}

func NewTestingLogWriter(t *testing.T) *TestingLogWriter {
	return &TestingLogWriter{t: t}
}

func (w *TestingLogWriter) Write(bs []byte) (int, error) {
	w.t.Log(string(bs))
	return len(bs), nil
}
