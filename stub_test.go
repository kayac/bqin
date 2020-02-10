package bqin_test

import (
	"testing"

	"github.com/kayac/bqin"
	"github.com/kayac/bqin/cloud"
	"github.com/kayac/bqin/internal/stub"
)

const (
	StubReceiptHandle = "MbZj6wDWli+JvwwJaBV+3dcjk2YW2vA3+STFFljTM8tJJg6HRG6PYSasuWXPJB+CwLj1FjgXUv1uSj1gUPAWV66FU/WeR4mq2OKpEGYWbnLmpRCJVAyeMjeU5ZBdtcQ+QEauMZc8ZRv37sIW2iJKq3M9MFx1YvV11A2x/KSbkJ0"
)

func NewReceiverWithStub(t *testing.T, conf *bqin.Config) (*bqin.SQSReceiver, func()) {
	stubSQS := stub.NewStubSQS(StubReceiptHandle)
	conf.Cloud.AWS.DisableSSL = true
	conf.Cloud.AWS.SQSEndpoint = stubSQS.Endpoint()
	conf.Cloud.AWS.AccessKeyID = "ACCESS_KEY_ID"
	conf.Cloud.AWS.SecretAccessKey = "SECRET_ACCESS_KEY"
	conf.Cloud.AWS.DisableShardConfigState = true

	receiver, err := bqin.NewSQSReceiver(conf, cloud.New(conf.Cloud))
	if err != nil {
		t.Fatal(err)
	}
	return receiver, stubSQS.Close
}

func NewProcessorWithStub(t *testing.T, conf *bqin.Config) (*bqin.BigQueryTransporter, func()) {

	stubS3 := stub.NewStubS3("testdata/s3/")
	stubGCS := stub.NewStubGCS()
	stubBigQuery := stub.NewStubBigQuery()
	closer := func() {
		stubS3.Close()
		stubGCS.Close()
		stubBigQuery.Close()
	}
	conf.Cloud.AWS.DisableSSL = true
	conf.Cloud.AWS.S3Endpoint = stubS3.Endpoint()
	conf.Cloud.AWS.AccessKeyID = "ACCESS_KEY_ID"
	conf.Cloud.AWS.SecretAccessKey = "SECRET_ACCESS_KEY"
	conf.Cloud.AWS.DisableShardConfigState = true
	conf.Cloud.AWS.S3ForcePathStyle = true
	conf.Cloud.GCP.WithoutAuthentication = true
	conf.Cloud.GCP.BigQueryEndpoint = stubBigQuery.Endpoint()
	conf.Cloud.GCP.CloudStorageEndpoint = stubGCS.Endpoint()

	return bqin.NewBigQueryTransporter(conf, cloud.New(conf.Cloud)), closer
}
