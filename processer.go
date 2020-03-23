package bqin

import (
	"context"
	"net/url"

	"github.com/kayac/bqin/cloud"
	"github.com/pkg/errors"
)

type BigQueryTransporter struct {
	*Transporter
	*Loader
}

func NewBigQueryTransporter(conf *Config, _ *cloud.Cloud) *BigQueryTransporter {
	return &BigQueryTransporter{
		Transporter: NewTransporter(
			conf.Cloud.AWS.BuildSession(),
			conf.Cloud.GCP.BuildOption(cloud.CloudStorageServiceID)...,
		),
		Loader: NewLoader(
			conf.Cloud.GCP.BuildOption(cloud.BigQueryServiceID)...,
		),
	}
}

func (t *BigQueryTransporter) Process(ctx context.Context, req *ImportRequest) error {

	for _, record := range req.Records {
		transportJob := &TransportJob{
			Source: &url.URL{
				Scheme: "s3",
				Host:   record.Source.Bucket,
				Path:   record.Source.Object,
			},
			Destination: &url.URL{
				Scheme: "gs",
				Host:   record.Option.TemporaryBucket,
				Path:   record.Source.Object,
			},
		}
		handle, err := t.Transporter.Transport(ctx, transportJob)
		if err != nil {
			return errors.Wrap(err, "Process.transport failed")
		}
		defer handle.Cleanup(ctx)

		loagindDestination := &LoadingDestination{
			ProjectID: record.Target.ProjectID,
			Dataset:   record.Target.Dataset,
			Table:     record.Target.Table,
		}
		loadingJob := NewLoadingJob(loagindDestination, transportJob.Destination.String())
		loadingJob.GCSRef.Compression = record.Option.getCompression()
		loadingJob.GCSRef.AutoDetect = record.Option.getAutoDetect()
		loadingJob.GCSRef.SourceFormat = record.Option.getSourceFormat()

		if err := t.Loader.Load(ctx, loadingJob); err != nil {
			return errors.Wrap(err, "Process.load failed")
		}
	}
	return nil
}
