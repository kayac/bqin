package bqin

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/gorilla/mux"
	"google.golang.org/api/option"
)

func NewMockedReceiver(t *testing.T, conf *Config) (*SQSReceiver, func()) {
	svc, closer := newMockedSQS(t)
	receiver, err := newSQSReceiver(conf, svc)
	if err != nil {
		t.Fatal(err)
	}
	return receiver, closer
}

func NewMockedTransporter(t *testing.T, conf *Config) (*BigQueryTransporter, func()) {

	s3svc, s3closer := newMockedS3(t)
	gcs, gcscloser := newMockedGCS(t)
	bq, bqcloser := newMockedBQ(t, conf)
	closer := func() {
		s3closer()
		gcscloser()
		bqcloser()
	}
	return newBigQueryTransporter(
		conf,
		s3svc,
		gcs,
		bq,
	), closer
}

const (
	// see https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_GetQueueUrl.html
	mockedGetQueueUrlResponseTmpl = `
<GetQueueUrlResponse>
    <GetQueueUrlResult>
        <QueueUrl>%s</QueueUrl>
    </GetQueueUrlResult>
    <ResponseMetadata>
        <RequestId>470a6f13-2ed9-4181-ad8a-2fdea142988e</RequestId>
    </ResponseMetadata>
</GetQueueUrlResponse>
`
	MockedReceiptHandle = "MbZj6wDWli+JvwwJaBV+3dcjk2YW2vA3+STFFljTM8tJJg6HRG6PYSasuWXPJB+CwLj1FjgXUv1uSj1gUPAWV66FU/WeR4mq2OKpEGYWbnLmpRCJVAyeMjeU5ZBdtcQ+QEauMZc8ZRv37sIW2iJKq3M9MFx1YvV11A2x/KSbkJ0"

	// see https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
	mockedReceiveMessageResponseTmpl = `
<ReceiveMessageResponse>
  <ReceiveMessageResult>
    <Message>
      <MessageId>5fea7756-0ea4-451a-a703-a558b933e274</MessageId>
      <ReceiptHandle>%s</ReceiptHandle>
      <MD5OfBody>%x</MD5OfBody>
      <Body>%s</Body>
      <Attribute>
        <Name>SenderId</Name>
        <Value>195004372649</Value>
      </Attribute>
      <Attribute>
        <Name>SentTimestamp</Name>
        <Value>1238099229000</Value>
      </Attribute>
      <Attribute>
        <Name>ApproximateReceiveCount</Name>
        <Value>5</Value>
      </Attribute>
      <Attribute>
        <Name>ApproximateFirstReceiveTimestamp</Name>
        <Value>1250700979248</Value>
      </Attribute>
    </Message>
  </ReceiveMessageResult>
  <ResponseMetadata>
    <RequestId>b6633655-283d-45b4-aee4-4e84e0ae6afa</RequestId>
  </ResponseMetadata>
</ReceiveMessageResponse>
`

	// see https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html
	mockedDeleteMessageResponseTmpl = `
<DeleteMessageResponse>
    <ResponseMetadata>
        <RequestId>b5293cb5-d306-4a17-9048-b263635abe42</RequestId>
    </ResponseMetadata>
</DeleteMessageResponse>
`
)

func newMockedSQS(t *testing.T) (*sqs.SQS, func()) {
	var endpoint string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("request to sqs: %s %s", r.Method, r.URL)
		bs, _ := ioutil.ReadAll(r.Body)
		paramsString := string(bs)
		t.Logf("Body: %s", paramsString)
		params, err := url.ParseQuery(paramsString)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		switch params.Get("Action") {
		case "GetQueueUrl":
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, fmt.Sprintf(mockedGetQueueUrlResponseTmpl, endpoint+"/queue/"+params.Get("QueueName")))
		case "ReceiveMessage":
			w.WriteHeader(http.StatusOK)
			bodyBs, _ := ioutil.ReadFile("testdata/sqs_msg/s3_object_created_put.json")
			io.WriteString(w,
				fmt.Sprintf(mockedReceiveMessageResponseTmpl, MockedReceiptHandle, md5.Sum(bodyBs), string(bodyBs)),
			)
		case "DeleteMessage":
			handle := params.Get("ReceiptHandle")
			if handle != MockedReceiptHandle {
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, "ReceiptHandleIsInvalid")
				return
			}
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, mockedDeleteMessageResponseTmpl)
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	endpoint = server.URL
	sess := session.Must(session.NewSession(&aws.Config{
		Credentials: credentials.NewStaticCredentials("DUMMY_AWS_KEY_ID", "DUMMY_AWS_SECRET_KEY", ""),
		DisableSSL:  aws.Bool(true),
		Endpoint:    aws.String(server.URL),
		Region:      aws.String("ap-northeast-1"),
	}))
	return sqs.New(sess), server.Close
}

func newMockedS3(t *testing.T) (*s3.S3, func()) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("request to s3: %s %s", r.Method, r.URL)
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		testdataPath := "testdata/s3/" + r.URL.Path
		body, err := os.Open(testdataPath)
		if err != nil {
			log.Print(err)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		io.Copy(w, body)
	}))

	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials("DUMMY_AWS_KEY_ID", "DUMMY_AWS_SECRET_KEY", ""),
		DisableSSL:       aws.Bool(true),
		Endpoint:         aws.String(server.URL),
		Region:           aws.String("ap-northeast-1"),
		S3ForcePathStyle: aws.Bool(true),
	}))
	return s3.New(sess), server.Close
}

func newMockedGCS(t *testing.T) (*storage.Client, func()) {

	mockedMux := mux.NewRouter()
	mockedMux.HandleFunc("/b/{bucket_id}", func(w http.ResponseWriter, r *http.Request) {
		// mocked Buckets: get API
		// https://cloud.google.com/storage/docs/json_api/v1/buckets/get?hl=ja
		encoder := json.NewEncoder(w)
		w.WriteHeader(http.StatusOK)
		params := mux.Vars(r)
		encoder.Encode(&mockedGCSBucket{
			Kind:         "storage#bucket",
			ID:           params["bucket_id"],
			Name:         params["bucket_id"],
			Location:     "ASIA-NORTHEAST1",
			LocationType: "region",
		})

	}).Methods("GET")
	mockedMux.HandleFunc("/b/{bucket_id}/o", func(w http.ResponseWriter, r *http.Request) {
		// mocked Objects:insert API
		// https://cloud.google.com/storage/docs/json_api/v1/objects/insert?hl=ja
		if r.URL.Query().Get("uploadType") != "multipart" {
			t.Logf("[mocked gcs API]: unexpected uploadType : %#v", r.URL.Query().Get("uploadType"))
			w.WriteHeader(http.StatusForbidden)
			return
		}
		boundary := strings.TrimPrefix(r.Header["Content-Type"][0], "multipart/related; boundary=")
		reader := multipart.NewReader(r.Body, boundary)
		part, err := reader.NextPart()
		if err != nil {
			t.Logf("[mocked gcs API]: can not decorde as multipart request : %v", err)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		bs := make([]byte, 256)
		n, _ := part.Read(bs)
		var data map[string]string
		json.Unmarshal(bs[0:n], &data)
		t.Logf("[mocked gcs API]:upload palyload :%v", data)
		w.WriteHeader(http.StatusOK)
	}).Methods("POST")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("request to gcs: %s %s", r.Method, r.URL)
		mockedMux.ServeHTTP(w, r)
	}))

	gcs, err := storage.NewClient(
		context.Background(),
		option.WithoutAuthentication(),
		option.WithEndpoint(server.URL),
	)
	if err != nil {
		t.Fatalf("prepare moeckd gcs error: %v", err)
	}
	return gcs, server.Close
}

func newMockedBQ(t *testing.T, conf *Config) (*bigquery.Client, func()) {
	mockedMux := mux.NewRouter()
	createdJobs := make(map[string]mockedBQJob, 1)
	mockedMux.HandleFunc("/projects/{dummy}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "Project id is missing, invalid")
	})
	mockedMux.HandleFunc("/projects/{project_id}/jobs", func(w http.ResponseWriter, r *http.Request) {
		//mock jobs.insert API
		// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/insert?hl=ja
		encoder := json.NewEncoder(w)
		var job mockedBQJob
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&job); err != nil {
			t.Errorf("[mocked bq jobs:insert API]: can not decode job object: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		if job.Configuration.Load == nil {
			t.Errorf("[mocked bq jobs:insert API]: unexpected jobType: %#v", job.Configuration)
		}
		job.Configuration.JobType = "LOAD"
		job.ID = job.JobReference.JobID
		sources := job.Configuration.Load.SourceUris
		for _, s := range sources {
			sURI, err := url.Parse(s)
			if err != nil {
				t.Errorf("[mocked bq jobs:insert API]: invalid sourceURI: %v", s)
				w.WriteHeader(http.StatusBadRequest)
				respErr := &mockedBQErrorProto{
					Message: "msg",
					Reason:  "reson",
				}
				job.Status = &mockedBQJobStatus{
					State:       "DONE",
					Errors:      []mockedBQErrorProto{*respErr},
					ErrorResult: respErr,
				}
				encoder.Encode(job)
				return
			}
			if sURI.Scheme != "gs" {
				t.Errorf("[mocked bq jobs:insert API]: invalid scheme: %v", s)
				w.WriteHeader(http.StatusBadRequest)
				respErr := &mockedBQErrorProto{
					Message: "msg",
					Reason:  "reson",
				}
				job.Status = &mockedBQJobStatus{
					State:       "DONE",
					Errors:      []mockedBQErrorProto{*respErr},
					ErrorResult: respErr,
				}
				encoder.Encode(job)
				return

			}
		}

		job.Status = &mockedBQJobStatus{State: "PENDING"}
		createdJobs[job.ID] = job
		w.WriteHeader(http.StatusOK)
		encoder.Encode(job)
		t.Logf("[mocked bq jobs:insert API]: job created id = %s", job.ID)
	}).Methods("POST")

	mockedMux.HandleFunc("/projects/{project_id}/jobs/{job_id}", func(w http.ResponseWriter, r *http.Request) {
		// mock jobs:get API
		// https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs/get?hl=ja
		params := mux.Vars(r)
		job, ok := createdJobs[params["job_id"]]
		if !ok {
			t.Log(createdJobs)
			t.Logf("[mocked bq jobs:get API]: job not found id = %s", params["job_id"])
			w.WriteHeader(http.StatusNotFound)
			return
		}
		job.Status.State = "DONE"
		w.WriteHeader(http.StatusOK)
		encoder := json.NewEncoder(w)
		if err := encoder.Encode(job); err != nil {
			t.Errorf("[mocked bq jobs:get API]: can not encode job object: %v", err)
			return
		}
		t.Logf("[mocked bq jobs:get API]: job done id = %s", job.ID)

	}).Methods("GET")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("request to bq: %s %s", r.Method, r.URL)
		mockedMux.ServeHTTP(w, r)
	}))

	bq, err := bigquery.NewClient(
		context.Background(),
		conf.GCP.ProjectID,
		option.WithoutAuthentication(),
		option.WithEndpoint(server.URL),
	)
	if err != nil {
		t.Fatalf("prepare moeckd bq error: %v", err)
	}
	return bq, server.Close
}
