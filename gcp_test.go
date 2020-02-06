package bqin

//use from mock
type mockedGCSBucket struct {
	Kind         string `json:"kind"`
	ID           string `json:"id"`
	Name         string `json:"name"`
	Location     string `json:"location"`
	LocationType string `json:"locationType"`
}

//as https://cloud.google.com/bigquery/docs/reference/rest/v2/Job?hl=ja
type mockedBQJob struct {
	Kind          string                   `json:"kind"`
	Etag          string                   `json:"etag"`
	ID            string                   `json:"id"`
	SelfLink      string                   `json:"selfLink"`
	UserEmail     string                   `json:"user_email"`
	Configuration mockedBQJobConfiguration `json:"configuration"`
	JobReference  *mockedBQJobReference    `json:"jobReference"`
	Statistics    interface{}              `json:"statistics"`
	Status        *mockedBQJobStatus       `json:"status"`
}

//as https://cloud.google.com/bigquery/docs/reference/rest/v2/Job?hl=ja#JobStatus
type mockedBQJobStatus struct {
	ErrorResult *mockedBQErrorProto  `json:"errorResult"`
	Errors      []mockedBQErrorProto `json:"errors"`

	//Output only. Running state of the job. Valid states include 'PENDING', 'RUNNING', and 'DONE'.
	State string `json:"state"`
}

//as https://cloud.google.com/bigquery/docs/reference/rest/v2/ErrorProto?hl=ja
type mockedBQErrorProto struct {
	Reason    string `json:"reason"`
	Location  string `json:"location"`
	DebugInfo string `json:"debugInfo"`
	Message   string `json:"message"`
}

//as https://cloud.google.com/bigquery/docs/reference/rest/v2/JobReference?hl=ja
type mockedBQJobReference struct {
	ProjectID string  `json:"projectId"`
	JobID     string  `json:"jobId"`
	Location  *string `json:"location"`
}

//as https://cloud.google.com/bigquery/docs/reference/rest/v2/Job?hl=ja#JobConfiguration
type mockedBQJobConfiguration struct {
	JobType      string                        `json:"jobType"`
	Query        interface{}                   `json:"query,omitempty"`
	Load         *mockedBQJobConfigurationLoad `json:"load,omitempty"`
	Copy         interface{}                   `json:"copy,omitempty"`
	Extract      interface{}                   `json:"extract,omitempty"`
	DryRun       bool                          `json:"dryRun"`
	JobTimeoutMs string                        `json:"jobTimeoutMs,omitempty"`
	Labels       map[string]string             `json:"labels,omitempty"`
}

//as https://cloud.google.com/bigquery/docs/reference/rest/v2/Job?hl=ja#JobConfigurationLoad
type mockedBQJobConfigurationLoad struct {
	SourceUris                         []string    `json:"sourceUris"`
	Schema                             interface{} `json:"schema"`
	DestinationTable                   interface{} `json:"destinationTable"`
	DestinationTableProperties         interface{} `json:"destinationTableProperties"`
	CreateDisposition                  string      `json:"createDisposition"`
	WriteDisposition                   string      `json:"writeDisposition"`
	NullMarker                         string      `json:"nullMarker"`
	FieldDelimiter                     string      `json:"fieldDelimiter"`
	SkipLeadingRows                    int         `json:"skipLeadingRows"`
	Encoding                           string      `json:"encoding"`
	Quote                              string      `json:"quote"`
	MaxBadRecords                      int         `json:"maxBadRecords"`
	SchemaInlineFormat                 string      `json:"schemaInlineFormat"`
	SchemaInline                       string      `json:"schemaInline"`
	AllowQuotedNewlines                bool        `json:"allowQuotedNewlines"`
	SourceFormat                       string      `json:"sourceFormat"`
	AllowJaggedRows                    bool        `json:"allowJaggedRows"`
	IgnoreUnknownValues                bool        `json:"ignoreUnknownValues"`
	ProjectionFields                   []string    `json:"projectionFields"`
	Autodetect                         bool        `json:"autodetect"`
	SchemaUpdateOptions                []string    `json:"schemaUpdateOptions"`
	TimePartitioning                   interface{} `json:"timePartitioning"`
	RangePartitioning                  interface{} `json:"rangePartitioning"`
	Clustering                         interface{} `json:"clustering"`
	DestinationEncryptionConfiguration interface{} `json:"destinationEncryptionConfiguration"`
	UseAvroLogicalTypes                bool        `json:"useAvroLogicalTypes"`
	HivePartitioningOptions            interface{} `json:"hivePartitioningOptions"`
}
