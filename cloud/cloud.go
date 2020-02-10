package cloud

import (
	"context"
	"sync"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"google.golang.org/api/option"
)

// Cloud pools and keeps clients connected to AWS and GCP.
type Cloud struct {
	awsSession   *session.Session
	bigqueryOpts []option.ClientOption
	storageOpts  []option.ClientOption

	mu     sync.Mutex
	s3svc  *s3.S3
	sqssvc *sqs.SQS
	gcs    *storage.Client
	bqs    map[string]*bigquery.Client
}

func New(conf *Config) *Cloud {
	return &Cloud{
		awsSession:   conf.AWS.BuildSession(),
		bigqueryOpts: conf.GCP.BuildOption(BigQueryServiceID),
		storageOpts:  conf.GCP.BuildOption(CloudStorageServiceID),
	}
}

func (c *Cloud) GetSQS() *sqs.SQS {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sqssvc == nil {
		c.sqssvc = sqs.New(c.awsSession)
	}
	return c.sqssvc
}

func (c *Cloud) GetS3() *s3.S3 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.s3svc == nil {
		c.s3svc = s3.New(c.awsSession)
	}
	return c.s3svc
}

func (c *Cloud) GetCloudStorageClient(ctx context.Context) (*storage.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.gcs == nil {
		var err error
		c.gcs, err = storage.NewClient(
			ctx,
			c.storageOpts...,
		)
		if err != nil {
			return nil, err
		}
	}
	return c.gcs, nil
}

func (c *Cloud) GetBigQueryClient(ctx context.Context, projectID string) (*bigquery.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bqs == nil {
		c.bqs = make(map[string]*bigquery.Client, 1)
	}
	if bq, ok := c.bqs[projectID]; ok {
		return bq, nil
	}
	bq, err := bigquery.NewClient(
		ctx,
		projectID,
		c.bigqueryOpts...,
	)
	if err == nil {
		c.bqs[projectID] = bq
	}
	return bq, err
}
