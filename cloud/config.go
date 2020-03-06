package cloud

import (
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"

	"google.golang.org/api/option"
)

type Config struct {
	AWS *AWS `yaml:"aws,omitempty"`
	GCP *GCP `yaml:"gcp,omitempty"`
}

func NewDefaultConfig() *Config {
	return &Config{
		AWS: &AWS{
			Region:           os.Getenv("AWS_REGION"),
			DisableSSL:       false,
			S3ForcePathStyle: false,
			S3Endpoint:       "",
			SQSEndpoint:      "",
		},
		GCP: &GCP{
			WithoutAuthentication: false,
		},
	}
}

type AWS struct {
	Region                  string `yaml:"region,omitempty"`
	DisableSSL              bool   `yaml:"disable_ssl,omitempty"`
	S3ForcePathStyle        bool   `yaml:"s3_force_path_style,omitempty"`
	S3Endpoint              string `yaml:"s3_endpoint,omitempty"`
	SQSEndpoint             string `yaml:"sqs_endpoint,omitempty"`
	AccessKeyID             string `yaml:"access_key_id,omitempty"`
	SecretAccessKey         string `yaml:"secret_access_key,omitempty"`
	DisableShardConfigState bool   `yaml:"disable_shard_config_state,omitempty"`
}

func (c *AWS) BuildSession() *session.Session {

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

const (
	BigQueryServiceID     = "bigquery"
	CloudStorageServiceID = "storage"
)

type GCP struct {
	WithoutAuthentication bool   `yaml:"without_authentication,omitempty"`
	BigQueryEndpoint      string `yaml:"big_query_endpoint,omitempty"`
	CloudStorageEndpoint  string `yaml:"cloud_storage_endpoint,omitempty"`
	Credential            string `yaml:"credential"`
}

func (c *GCP) BuildOption(service string) []option.ClientOption {
	opts := make([]option.ClientOption, 0, 2)
	if c.WithoutAuthentication {
		opts = append(opts, option.WithoutAuthentication())
	}
	if c.Credential != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(c.Credential)))
	}
	switch service {
	case BigQueryServiceID:
		if c.BigQueryEndpoint != "" {
			opts = append(opts, option.WithEndpoint(c.BigQueryEndpoint))
		}
	case CloudStorageServiceID:
		if c.CloudStorageEndpoint != "" {
			opts = append(opts, option.WithEndpoint(c.CloudStorageEndpoint))
		}
	}
	return opts
}
