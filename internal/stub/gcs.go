package stub

import (
	"encoding/json"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/kayac/bqin/internal/logger"
)

type StubGCS struct {
	stub
	storage map[string]bool
}

func NewStubGCS() *StubGCS {
	s := &StubGCS{}
	s.setSvcName("gcs")
	r := s.getRouter()
	r.HandleFunc("/b/{bucket_id}", s.serveGetBucket).Methods("GET")
	r.HandleFunc("/b/{bucket_id}/o", s.serveInsertObject).Methods("POST")
	return s
}

type StubGCSGetBucketResponse struct {
	Kind         string `json:"kind"`
	ID           string `json:"id"`
	Name         string `json:"name"`
	Location     string `json:"location"`
	LocationType string `json:"locationType"`
}

// see https://cloud.google.com/storage/docs/json_api/v1/buckets/get?hl=ja
func (s *StubGCS) serveGetBucket(w http.ResponseWriter, r *http.Request) {
	encoder := json.NewEncoder(w)
	w.WriteHeader(http.StatusOK)
	params := mux.Vars(r)
	encoder.Encode(&StubGCSGetBucketResponse{
		Kind:         "storage#bucket",
		ID:           params["bucket_id"],
		Name:         params["bucket_id"],
		Location:     "ASIA-NORTHEAST1",
		LocationType: "region",
	})
}

// see // https://cloud.google.com/storage/docs/json_api/v1/objects/insert?hl=ja
func (s *StubGCS) serveInsertObject(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("uploadType") != "multipart" {
		logger.Debugf("[stub_gcs]: unexpected uploadType : %s", r.URL.Query().Get("uploadType"))
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	boundary := strings.TrimPrefix(r.Header["Content-Type"][0], "multipart/related; boundary=")
	reader := multipart.NewReader(r.Body, boundary)
	part, err := reader.NextPart()
	if err != nil {
		logger.Debugf("[stub_gcs]: can not decorde as multipart request : %s", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	bs := make([]byte, 256)
	n, _ := part.Read(bs)
	var meta map[string]string
	json.Unmarshal(bs[0:n], &meta)
	logger.Debugf("[stub_gcs]:upload palyload :%v", meta)
	w.WriteHeader(http.StatusOK)
}
