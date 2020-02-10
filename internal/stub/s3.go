package stub

import (
	"io"
	"net/http"
	"os"

	"github.com/kayac/bqin/internal/logger"
)

type StubS3 struct {
	stub
	basePath string
}

func NewStubS3(basePath string) *StubS3 {
	s := &StubS3{basePath: basePath}
	s.setSvcName("s3")
	r := s.getRouter()
	r.PathPrefix("/").HandlerFunc(s.serveObject).Methods("GET")
	return s
}

func (s *StubS3) serveObject(w http.ResponseWriter, r *http.Request) {
	testdataPath := s.basePath + r.URL.Path
	body, err := os.Open(testdataPath)
	if err != nil {
		logger.Debugf("[stub_s3] %s", err)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	io.Copy(w, body)
}
