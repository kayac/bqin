package bqin

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/kayac/bqin/internal/logger"
	"github.com/pkg/errors"
)

type BigQueryTransporter struct {
	s3svc *s3.S3
	gcs   *storage.Client
	bq    *bigquery.Client

	temporaryBucket string
}

func NewBigQueryTransporter(conf *Config, sess *session.Session) (*BigQueryTransporter, error) {
	ctx := context.Background()
	gcs, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	bq, err := bigquery.NewClient(ctx, conf.GCP.ProjectID)
	if err != nil {
		return nil, err
	}

	return newBigQueryTransporter(
		conf,
		s3.New(sess),
		gcs,
		bq,
	), nil
}

func newBigQueryTransporter(conf *Config, s3svc *s3.S3, gcs *storage.Client, bq *bigquery.Client) *BigQueryTransporter {
	return &BigQueryTransporter{
		s3svc:           s3svc,
		gcs:             gcs,
		bq:              bq,
		temporaryBucket: conf.GCSTemporaryBucket,
	}
}

func (t *BigQueryTransporter) Process(ctx context.Context, req *ImportRequest) error {

	bucket, err := t.check(ctx)
	if err != nil {
		return errors.Wrap(err, "Process.check condition invalid")
	}
	for _, record := range req.Records {
		obj, err := t.transfer(ctx, record, bucket)
		if err != nil {
			return errors.Wrap(err, "Process.transfer")
		}
		if err := t.load(ctx, record); err != nil {
			return errors.Wrap(err, "Process.load")
		}
		if err := t.cleanup(ctx, obj); err != nil {
			return errors.Wrap(err, "Process.cleanup")
		}
	}
	return nil
}

func (t *BigQueryTransporter) check(ctx context.Context) (*storage.BucketHandle, error) {
	if t.temporaryBucket == "" {
		return nil, errors.New("gcs temporary bucket name is missing. invalid config")
	}
	bucket := t.gcs.Bucket(t.temporaryBucket)
	attr, err := bucket.Attrs(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "gcs temporary bucket attribute can not check")
	}
	logger.Debugf("temporary bucket name is %s", attr.Name)
	logger.Debugf("temporary bucket location is %s (type:%s)", attr.Location, attr.LocationType)
	return bucket, nil
}

func (t *BigQueryTransporter) transfer(ctx context.Context, record *ImportRequestRecord, gbucket *storage.BucketHandle) (*storage.ObjectHandle, error) {

	resp, err := t.s3svc.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(record.Source.Bucket),
		Key:    aws.String(record.Source.Object),
	})
	if err != nil {
		return nil, errors.Wrap(err, "get object from s3 failed")
	}
	logger.Debugf("get object from %s successed.", record.Source)
	defer resp.Body.Close()

	obj := gbucket.Object(record.Source.Object)
	writer := obj.NewWriter(ctx)
	defer writer.Close()

	_, err = io.Copy(writer, resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "copy object failed")
	}
	logger.Debugf("transfer to %s successed.", t.tmpObjectURL(record))
	return obj, nil
}

func (t *BigQueryTransporter) tmpObjectURL(record *ImportRequestRecord) string {
	return fmt.Sprintf("gs://%s/%s", t.temporaryBucket, record.Source.Object)
}

func (t *BigQueryTransporter) load(ctx context.Context, record *ImportRequestRecord) error {
	gcsRef := bigquery.NewGCSReference(t.tmpObjectURL(record))
	gcsRef.AutoDetect = true
	gcsRef.MaxBadRecords = 100
	gcsRef.AllowJaggedRows = true
	logger.Debugf("prepre gcs reference: dump is %+v", gcsRef)

	loader := t.bq.Dataset(record.Target.Dataset).Table(record.Target.Table).LoaderFrom(gcsRef)
	loader.CreateDisposition = bigquery.CreateIfNeeded

	job, err := loader.Run(ctx)
	if err != nil {
		return errors.Wrap(err, "create job failed")
	}
	jobID := job.ID()
	logger.Debugf("loader run successed. job id = %s", jobID)
	logger.Debugf("[job:%s] wating", jobID)
	status, err := job.Wait(ctx)
	if err != nil {
		return errors.Wrap(err, "can not wait job")
	}
	logger.Debugf("[job:%s] done.", jobID)
	if err := status.Err(); err != nil {
		return errors.Wrap(err, "job status error")
	}
	logger.Debugf("[job:%s] successed.", jobID)
	return nil
}

func (t *BigQueryTransporter) cleanup(ctx context.Context, obj *storage.ObjectHandle) error {
	logger.Debugf("cleanup temporary object gs://%s/%s", obj.BucketName(), obj.ObjectName())
	err := obj.Delete(ctx)
	if err != nil {
		if err != storage.ErrObjectNotExist {
			logger.Errorf("can not delete temporary object reason: %s", err)
			return err
		}
		logger.Debugf("aleady removed gs://%s/%s", obj.BucketName(), obj.ObjectName())
	}
	logger.Debugf("cleanup finish.")
	return nil
}
