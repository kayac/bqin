package stub

import (
	"encoding/json"
	"net/http"
)

type AccessLog struct {
	Svc        string `json:"svc,omitempty"`
	Method     string `json:"method"`
	Path       string `json:"path"`
	StatusCode int    `json:"status_code"`
	SQSAction  string `json:"sqs_action,omitempty"`
}

func (l *AccessLog) String() string {
	s, _ := json.Marshal(l)
	return string(s)
}

type stubResponseWriter struct {
	http.ResponseWriter
	log *AccessLog
}

func (w *stubResponseWriter) WriteHeader(statusCode int) {
	w.ResponseWriter.WriteHeader(statusCode)
	w.log.StatusCode = statusCode
}
