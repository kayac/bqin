package stub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/gorilla/mux"
	"github.com/kayac/bqin/internal/logger"
	"google.golang.org/api/option"
)

type stub struct {
	server  *httptest.Server
	router  *mux.Router
	logs    []*AccessLog
	mu      sync.Mutex
	svcName string
}

func (s *stub) GetLogs() []string {
	ret := make([]string, len(s.logs))
	for i, l := range s.logs {
		ret[i] = l.String()
	}
	return ret
}

func (s *stub) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server != nil {
		s.server.Close()
	}
}

func (s *stub) setSvcName(name string) {
	s.svcName = name
}

func (s *stub) getServer() *httptest.Server {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.server == nil {
		s.server = httptest.NewServer(http.HandlerFunc(s.handle))
		s.logs = make([]*AccessLog, 0, 10)
	}
	return s.server
}

func (s *stub) getRouter() *mux.Router {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.router == nil {
		s.router = mux.NewRouter()
	}
	return s.router
}

func (s *stub) handle(w http.ResponseWriter, r *http.Request) {
	ww, rr, l := s.wrap(w, r)
	s.getRouter().ServeHTTP(ww, rr)
	logger.Debugf("%s", l)
}

func (s *stub) wrap(w http.ResponseWriter, r *http.Request) (http.ResponseWriter, *http.Request, *AccessLog) {
	l := &AccessLog{
		Method: r.Method,
		Path:   r.URL.Path,
		Svc:    s.svcName,
	}
	ww := &stubResponseWriter{ResponseWriter: w, log: l}
	ctx := r.Context()
	rr := r.WithContext(context.WithValue(ctx, "__accesslog", l))
	s.logs = append(s.logs, l)
	return ww, rr, l
}

func (s *stub) getAccessLog(r *http.Request) *AccessLog {
	l := r.Context().Value("__accesslog")
	return l.(*AccessLog)
}

func (s *stub) getAWSSession() *session.Session {
	endpoint := s.getServer().URL
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials("AWS_KEY_ID", "AWS_SECRET_KEY", ""),
		DisableSSL:       aws.Bool(true),
		Endpoint:         aws.String(endpoint),
		Region:           aws.String("ap-northeast-1"),
		S3ForcePathStyle: aws.Bool(true),
	}))
	return sess
}

func (s *stub) getGCPClientOptions() []option.ClientOption {
	endpoint := s.getServer().URL
	return []option.ClientOption{
		option.WithoutAuthentication(),
		option.WithEndpoint(endpoint),
	}
}
