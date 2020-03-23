package bqin

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/kayac/bqin/internal/logger"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
)

type Loader struct {
	opts []option.ClientOption
}

func NewLoader(opts ...option.ClientOption) *Loader {
	return &Loader{
		opts: opts,
	}
}

type LoadingDestination struct {
	ProjectID string
	Dataset   string
	Table     string
}

type LoadingJob struct {
	GCSRef *bigquery.GCSReference
	*LoadingDestination

	CreateDisposition bigquery.TableCreateDisposition
	WriteDisposition  bigquery.TableWriteDisposition
}

func NewLoadingJob(dest *LoadingDestination, objectURIs ...string) *LoadingJob {
	return &LoadingJob{
		GCSRef:             bigquery.NewGCSReference(objectURIs...),
		LoadingDestination: dest,
		CreateDisposition:  bigquery.CreateIfNeeded,
		WriteDisposition:   bigquery.WriteAppend,
	}
}

func (l *Loader) Load(ctx context.Context, job *LoadingJob) error {
	bq, err := bigquery.NewClient(ctx, job.ProjectID, l.opts...)
	if err != nil {
		return errors.Wrap(err, "can not get bigquery client")
	}

	loader := bq.Dataset(job.Dataset).Table(job.Table).LoaderFrom(job.GCSRef)
	loader.CreateDisposition = job.CreateDisposition
	loader.WriteDisposition = job.WriteDisposition
	bqjob, err := loader.Run(ctx)
	if err != nil {
		return errors.Wrap(err, "create job failed")
	}

	logger.Debugf("create load job successed. jon_id=%s", bqjob.ID())
	status, err := bqjob.Wait(ctx)
	if err != nil {
		return errors.Wrap(err, "can not wait job")
	}
	return errors.Wrap(status.Err(), "load job failed")
}
