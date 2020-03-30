package bqin_test

import (
	"net/url"
	"os"

	"github.com/kayac/bqin"
	"github.com/kayac/bqin/internal/logger"
	"github.com/kayac/bqin/internal/stub"
)

func GetLogLevel() string {
	debug := os.Getenv("DEBUG")
	if debug == "" {
		return logger.InfoLevel
	}
	return logger.DebugLevel
}

func MustParseURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return u
}

type StubManager struct {
	SQS          *stub.StubSQS
	S3           *stub.StubS3
	BigQuery     *stub.StubBigQuery
	CloudStorage *stub.StubGCS
}

func NewStubManager(basePath string) *StubManager {
	return &StubManager{
		SQS:          stub.NewStubSQS("hoge"),
		S3:           stub.NewStubS3(basePath),
		BigQuery:     stub.NewStubBigQuery(),
		CloudStorage: stub.NewStubGCS(),
	}
}

func (m *StubManager) Close() {
	m.SQS.Close()
	m.S3.Close()
	m.BigQuery.Close()
	m.CloudStorage.Close()
}

func (m *StubManager) OverwriteConfig(conf *bqin.Config) {
	conf.Cloud.AWS.DisableSSL = true
	conf.Cloud.AWS.S3Endpoint = m.S3.Endpoint()
	conf.Cloud.AWS.SQSEndpoint = m.SQS.Endpoint()
	conf.Cloud.AWS.AccessKeyID = "ACCESS_KEY_ID"
	conf.Cloud.AWS.SecretAccessKey = "SECRET_ACCESS_KEY"
	conf.Cloud.AWS.DisableShardConfigState = true
	conf.Cloud.AWS.S3ForcePathStyle = true
	conf.Cloud.GCP.WithoutAuthentication = true
	conf.Cloud.GCP.BigQueryEndpoint = m.BigQuery.Endpoint()
	conf.Cloud.GCP.CloudStorageEndpoint = m.CloudStorage.Endpoint()
}
