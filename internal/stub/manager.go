package stub

import "github.com/kayac/bqin/cloud"

type Manager struct {
	SQS          *StubSQS
	S3           *StubS3
	BigQuery     *StubBigQuery
	CloudStorage *StubGCS
}

func NewManager(basePath string) *Manager {
	return &Manager{
		SQS:          NewStubSQS("hoge"),
		S3:           NewStubS3(basePath),
		BigQuery:     NewStubBigQuery(),
		CloudStorage: NewStubGCS(),
	}
}

func (m *Manager) Close() {
	m.SQS.Close()
	m.S3.Close()
	m.BigQuery.Close()
	m.CloudStorage.Close()
}

func (m *Manager) OverwriteConfig(org *cloud.Config) {
	org.AWS.DisableSSL = true
	org.AWS.S3Endpoint = m.S3.Endpoint()
	org.AWS.SQSEndpoint = m.SQS.Endpoint()
	org.AWS.AccessKeyID = "ACCESS_KEY_ID"
	org.AWS.SecretAccessKey = "SECRET_ACCESS_KEY"
	org.AWS.DisableShardConfigState = true
	org.AWS.S3ForcePathStyle = true
	org.GCP.WithoutAuthentication = true
	org.GCP.BigQueryEndpoint = m.BigQuery.Endpoint()
	org.GCP.CloudStorageEndpoint = m.CloudStorage.Endpoint()
}
