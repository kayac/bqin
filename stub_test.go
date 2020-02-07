package bqin

import (
	"testing"

	"github.com/kayac/bqin/internal/stub"
)

const (
	StubReceiptHandle = "MbZj6wDWli+JvwwJaBV+3dcjk2YW2vA3+STFFljTM8tJJg6HRG6PYSasuWXPJB+CwLj1FjgXUv1uSj1gUPAWV66FU/WeR4mq2OKpEGYWbnLmpRCJVAyeMjeU5ZBdtcQ+QEauMZc8ZRv37sIW2iJKq3M9MFx1YvV11A2x/KSbkJ0"
)

func NewReceiverWithStub(t *testing.T, conf *Config) (*SQSReceiver, func()) {
	stubSQS := stub.NewStubSQS(StubReceiptHandle)
	receiver, err := newSQSReceiver(conf, stubSQS.NewSvc())
	if err != nil {
		t.Fatal(err)
	}
	return receiver, stubSQS.Close
}

func NewProcessorWithStub(t *testing.T, conf *Config) (*BigQueryTransporter, func()) {

	stubS3 := stub.NewStubS3("testdata/s3/")
	stubGCS := stub.NewStubGCS()
	stubBigQuery := stub.NewStubBigQuery()
	closer := func() {
		stubS3.Close()
		stubGCS.Close()
		stubBigQuery.Close()
	}
	gcs, err := stubGCS.NewClient()
	if err != nil {
		t.Fatalf("stub gcs setup failed")
	}
	bq, err := stubBigQuery.NewClient(conf.GCP.ProjectID)
	if err != nil {
		t.Fatalf("stub bigquery setup failed")
	}
	return newBigQueryTransporter(
		conf,
		stubS3.NewSvc(),
		gcs,
		bq,
	), closer
}
