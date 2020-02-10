package stub

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/gorilla/mux"
	"github.com/kayac/bqin/internal/logger"
)

type StubBigQuery struct {
	stub
	createdJobs map[string]*StubBigQueryResponseJob
	loaded      map[string][]string
}

func NewStubBigQuery() *StubBigQuery {
	s := &StubBigQuery{
		createdJobs: make(map[string]*StubBigQueryResponseJob, 1),
		loaded:      make(map[string][]string, 0),
	}
	s.setSvcName("bigquery")
	r := s.getRouter()
	r.HandleFunc("/projects/{dummy}", s.serveIfNotSetProjectID)
	r.HandleFunc("/projects/{project_id}/jobs/{job_id}", s.serveGetJob).Methods("GET")
	r.HandleFunc("/projects/{project_id}/jobs", s.serveInsertJobs).Methods("POST")
	return s
}

//if not set project id, always return bad request
func (s *StubBigQuery) serveIfNotSetProjectID(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusBadRequest)
	io.WriteString(w, "Project id is missing, invalid")
}

// see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert?hl=ja
func (s *StubBigQuery) serveInsertJobs(w http.ResponseWriter, r *http.Request) {
	encoder := json.NewEncoder(w)
	job := &StubBigQueryResponseJob{}
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(job); err != nil {
		logger.Debugf("[stub_bigquery] can not decode job object: %s", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if job.Configuration.Load == nil {
		logger.Debugf("[stub_bigquery] unsupported jobType: %#v", job.Configuration)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	job.Configuration.JobType = "LOAD"
	job.ID = job.JobReference.JobID
	sources := job.Configuration.Load.SourceUris
	for _, s := range sources {
		sURI, err := url.Parse(s)
		if err != nil {
			logger.Debugf("[stub_bigquery] invalid sourceURI: %v", s)
			w.WriteHeader(http.StatusBadRequest)
			respErr := &StubBigQueryResponseErrorProto{
				Message: "msg",
				Reason:  "reson",
			}
			job.Status = &StubBigQueryResponseJobStatus{
				State:       "DONE",
				Errors:      []StubBigQueryResponseErrorProto{*respErr},
				ErrorResult: respErr,
			}
			encoder.Encode(job)
			return
		}
		if sURI.Scheme != "gs" {
			logger.Debugf("[stub_bigquery] invalid scheme: %v", s)
			w.WriteHeader(http.StatusBadRequest)
			respErr := &StubBigQueryResponseErrorProto{
				Message: "msg",
				Reason:  "reson",
			}
			job.Status = &StubBigQueryResponseJobStatus{
				State:       "DONE",
				Errors:      []StubBigQueryResponseErrorProto{*respErr},
				ErrorResult: respErr,
			}
			encoder.Encode(job)
			return
		}
	}

	job.Status = &StubBigQueryResponseJobStatus{State: "PENDING"}
	s.createdJobs[job.ID] = job
	w.WriteHeader(http.StatusOK)
	encoder.Encode(job)
	logger.Debugf("[stub_bigquery] job created id = %s", job.ID)

}

// see https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/get?hl=ja
func (s *StubBigQuery) serveGetJob(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	job, ok := s.createdJobs[params["job_id"]]
	if !ok {
		logger.Debugf("[stub_bigquery] job not found id = %s", params["job_id"])
		w.WriteHeader(http.StatusNotFound)
		return
	}
	job.Status.State = "DONE"
	target := job.Configuration.Load.DestinationTable.String()
	if _, ok := s.loaded[target]; !ok {
		s.loaded[target] = make([]string, 0, len(job.Configuration.Load.SourceUris))
	}
	s.loaded[target] = append(s.loaded[target], job.Configuration.Load.SourceUris...)

	w.WriteHeader(http.StatusOK)
	encoder := json.NewEncoder(w)
	if err := encoder.Encode(job); err != nil {
		logger.Debugf("[stub_bigquery] can not encode job object: %v", err)
		return
	}
	logger.Debugf("[stub_bigquery] job done id = %s", job.ID)

}

func (s *StubBigQuery) LoadedData() map[string][]string {
	return s.loaded
}

//as https://cloud.google.com/bigquery/docs/reference/rest/v2/Job?hl=ja
type StubBigQueryResponseJob struct {
	Kind          string                               `json:"kind"`
	Etag          string                               `json:"etag"`
	ID            string                               `json:"id"`
	SelfLink      string                               `json:"selfLink"`
	UserEmail     string                               `json:"user_email"`
	Configuration StubBigQueryResponseJobConfiguration `json:"configuration"`
	JobReference  *StubBigQueryResponseJobReference    `json:"jobReference"`
	Statistics    interface{}                          `json:"statistics"`
	Status        *StubBigQueryResponseJobStatus       `json:"status"`
}

//as https://cloud.google.com/bigquery/docs/reference/rest/v2/Job?hl=ja#JobStatus
type StubBigQueryResponseJobStatus struct {
	ErrorResult *StubBigQueryResponseErrorProto  `json:"errorResult"`
	Errors      []StubBigQueryResponseErrorProto `json:"errors"`

	//Output only. Running state of the job. Valid states include 'PENDING', 'RUNNING', and 'DONE'.
	State string `json:"state"`
}

//as https://cloud.google.com/bigquery/docs/reference/rest/v2/ErrorProto?hl=ja
type StubBigQueryResponseErrorProto struct {
	Reason    string `json:"reason"`
	Location  string `json:"location"`
	DebugInfo string `json:"debugInfo"`
	Message   string `json:"message"`
}

//as https://cloud.google.com/bigquery/docs/reference/rest/v2/JobReference?hl=ja
type StubBigQueryResponseJobReference struct {
	ProjectID string  `json:"projectId"`
	JobID     string  `json:"jobId"`
	Location  *string `json:"location"`
}

//as https://cloud.google.com/bigquery/docs/reference/rest/v2/Job?hl=ja#JobConfiguration
type StubBigQueryResponseJobConfiguration struct {
	JobType      string                                    `json:"jobType"`
	Query        interface{}                               `json:"query,omitempty"`
	Load         *StubBigQueryResponseJobConfigurationLoad `json:"load,omitempty"`
	Copy         interface{}                               `json:"copy,omitempty"`
	Extract      interface{}                               `json:"extract,omitempty"`
	DryRun       bool                                      `json:"dryRun"`
	JobTimeoutMs string                                    `json:"jobTimeoutMs,omitempty"`
	Labels       map[string]string                         `json:"labels,omitempty"`
}

//as https://cloud.google.com/bigquery/docs/reference/rest/v2/Job?hl=ja#JobConfigurationLoad
type StubBigQueryResponseJobConfigurationLoad struct {
	SourceUris                         []string                              `json:"sourceUris"`
	Schema                             interface{}                           `json:"schema"`
	DestinationTable                   *StubBigQueryResponseDestinationTable `json:"destinationTable"`
	DestinationTableProperties         interface{}                           `json:"destinationTableProperties"`
	CreateDisposition                  string                                `json:"createDisposition"`
	WriteDisposition                   string                                `json:"writeDisposition"`
	NullMarker                         string                                `json:"nullMarker"`
	FieldDelimiter                     string                                `json:"fieldDelimiter"`
	SkipLeadingRows                    int                                   `json:"skipLeadingRows"`
	Encoding                           string                                `json:"encoding"`
	Quote                              string                                `json:"quote"`
	MaxBadRecords                      int                                   `json:"maxBadRecords"`
	SchemaInlineFormat                 string                                `json:"schemaInlineFormat"`
	SchemaInline                       string                                `json:"schemaInline"`
	AllowQuotedNewlines                bool                                  `json:"allowQuotedNewlines"`
	SourceFormat                       string                                `json:"sourceFormat"`
	AllowJaggedRows                    bool                                  `json:"allowJaggedRows"`
	IgnoreUnknownValues                bool                                  `json:"ignoreUnknownValues"`
	ProjectionFields                   []string                              `json:"projectionFields"`
	Autodetect                         bool                                  `json:"autodetect"`
	SchemaUpdateOptions                []string                              `json:"schemaUpdateOptions"`
	TimePartitioning                   interface{}                           `json:"timePartitioning"`
	RangePartitioning                  interface{}                           `json:"rangePartitioning"`
	Clustering                         interface{}                           `json:"clustering"`
	DestinationEncryptionConfiguration interface{}                           `json:"destinationEncryptionConfiguration"`
	UseAvroLogicalTypes                bool                                  `json:"useAvroLogicalTypes"`
	HivePartitioningOptions            interface{}                           `json:"hivePartitioningOptions"`
}

// as https://cloud.google.com/bigquery/docs/reference/rest/v2/TableReference?hl=ja
type StubBigQueryResponseDestinationTable struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	TableID   string `json:"tableId"`
}

func (t *StubBigQueryResponseDestinationTable) String() string {
	return fmt.Sprintf("%s.%s.%s", t.ProjectID, t.DatasetID, t.TableID)
}
