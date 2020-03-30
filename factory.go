package bqin

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"

	"google.golang.org/api/option"
)

type Factory struct {
	*Config
}

func (f *Factory) NewAWSSession() *session.Session {
	c := f.Config.Cloud.AWS
	conf := &aws.Config{
		DisableSSL:       aws.Bool(c.DisableSSL),
		Region:           aws.String(c.Region),
		S3ForcePathStyle: aws.Bool(c.S3ForcePathStyle),
	}
	if c.S3Endpoint != "" || c.SQSEndpoint != "" {
		defaultResolver := endpoints.DefaultResolver()
		customResolver := endpoints.ResolverFunc(
			func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
				if c.S3Endpoint != "" && service == endpoints.S3ServiceID {
					return endpoints.ResolvedEndpoint{
						URL:           c.S3Endpoint,
						SigningRegion: region,
					}, nil
				}
				if c.SQSEndpoint != "" && service == endpoints.SqsServiceID {
					return endpoints.ResolvedEndpoint{
						URL:           c.SQSEndpoint,
						SigningRegion: region,
					}, nil
				}
				return defaultResolver.EndpointFor(service, region, optFns...)
			},
		)
		conf = conf.WithEndpointResolver(customResolver)
	}
	if c.AccessKeyID != "" || c.SecretAccessKey != "" {
		conf = conf.WithCredentials(credentials.NewStaticCredentials(c.AccessKeyID, c.SecretAccessKey, ""))
	}
	var shardConfigState session.SharedConfigState = session.SharedConfigEnable
	if c.DisableShardConfigState {
		shardConfigState = session.SharedConfigDisable
	}
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config:            *conf,
		SharedConfigState: shardConfigState,
	}))
	return sess
}

func (f *Factory) NewCloudStorageOptions() []option.ClientOption {
	opts := f.NewGCPOptions()
	if endpoint := f.Config.Cloud.GCP.CloudStorageEndpoint; endpoint != "" {
		opts = append(opts, option.WithEndpoint(endpoint))
	}
	return opts
}

func (f *Factory) NewBigQueryOptions() []option.ClientOption {
	opts := f.NewGCPOptions()
	if endpoint := f.Config.Cloud.GCP.BigQueryEndpoint; endpoint != "" {
		opts = append(opts, option.WithEndpoint(endpoint))
	}
	return opts
}

func (f *Factory) NewGCPOptions() []option.ClientOption {
	c := f.Config.Cloud.GCP
	opts := make([]option.ClientOption, 0, 2)
	if c.WithoutAuthentication {
		opts = append(opts, option.WithoutAuthentication())
	}
	if !c.Base64Credential.IsEmpty() {
		opts = append(opts, option.WithCredentialsJSON(c.Base64Credential.Bytes()))
	}
	return opts
}

func (f *Factory) NewReceiver() *Receiver {
	return NewReceiver(
		f.Config.QueueName,
		f.NewAWSSession(),
	)
}

func (f *Factory) NewResolver() *Resolver {
	return NewResolver(
		f.Config.Rules,
	)
}

func (f *Factory) NewTransporter() *Transporter {
	return NewTransporter(
		f.NewAWSSession(),
		f.NewCloudStorageOptions()...,
	)
}

func (f *Factory) NewLoader() *Loader {
	return NewLoader(
		f.NewBigQueryOptions()...,
	)
}

func (f *Factory) NewApp() *App {
	return &App{
		Receiver:    f.NewReceiver(),
		Resolver:    f.NewResolver(),
		Transporter: f.NewTransporter(),
		Loader:      f.NewLoader(),
	}
}
