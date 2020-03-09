package bqin

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/kayac/bqin/cloud"
	"github.com/kayac/bqin/internal/logger"
	"github.com/pkg/errors"
)

type BigQueryTransporter struct {
	cloud *cloud.Cloud
	s3svc *s3.S3
	gcs   *storage.Client
	bq    *bigquery.Client
}

func NewBigQueryTransporter(conf *Config, c *cloud.Cloud) *BigQueryTransporter {
	return &BigQueryTransporter{
		cloud: c,
	}
}

func (t *BigQueryTransporter) Process(ctx context.Context, req *ImportRequest) error {

	for _, record := range req.Records {
		bucket, err := t.check(ctx, record.Option)
		if err != nil {
			return errors.Wrap(err, "Process.check condition invalid")
		}
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

func (t *BigQueryTransporter) check(ctx context.Context, opt *ImportOption) (*storage.BucketHandle, error) {
	gcs, err := t.cloud.GetCloudStorageClient(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "can not get cloud storage client")
	}
	bucket := gcs.Bucket(opt.TemporaryBucket)
	attr, err := bucket.Attrs(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "gcs temporary bucket attribute can not check")
	}
	logger.Debugf("temporary bucket name is %s", attr.Name)
	logger.Debugf("temporary bucket location is %s (type:%s)", attr.Location, attr.LocationType)
	return bucket, nil
}

func (t *BigQueryTransporter) transfer(ctx context.Context, record *ImportRequestRecord, bucket *storage.BucketHandle) (*storage.ObjectHandle, error) {

	resp, err := t.cloud.GetS3().GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(record.Source.Bucket),
		Key:    aws.String(record.Source.Object),
	})
	if err != nil {
		return nil, errors.Wrap(err, "get object from s3 failed")
	}
	logger.Debugf("get object from %s successed.", record.Source)
	defer resp.Body.Close()

	obj := bucket.Object(record.Source.Object)
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
	return fmt.Sprintf("gs://%s/%s", record.Option.TemporaryBucket, record.Source.Object)
}

func (t *BigQueryTransporter) load(ctx context.Context, record *ImportRequestRecord) error {
	gcsRef := bigquery.NewGCSReference(t.tmpObjectURL(record))
	gcsRef.AutoDetect = true
	gcsRef.MaxBadRecords = 100
	logger.Debugf("prepre gcs reference: dump is %+v", gcsRef)

	bq, err := t.cloud.GetBigQueryClient(ctx, record.Target.ProjectID)
	if err != nil {
		return errors.Wrap(err, "can not get bigquery client")
	}

	loader := bq.Dataset(record.Target.Dataset).Table(record.Target.Table).LoaderFrom(gcsRef)
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
