package bqin

import (
	"os"

	goconfig "github.com/kayac/go-config"
	"github.com/pkg/errors"
)

type Config struct {
	QueueName string `yaml:"queue_name"`
	Cloud     *Cloud `yaml:"cloud"`

	Rules []*Rule `yaml:"rules"`
	Rule  `yaml:",inline"`
}

type Cloud struct {
	AWS *AWS `yaml:"aws,omitempty"`
	GCP *GCP `yaml:"gcp,omitempty"`
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

type GCP struct {
	WithoutAuthentication bool         `yaml:"without_authentication,omitempty"`
	BigQueryEndpoint      string       `yaml:"big_query_endpoint,omitempty"`
	CloudStorageEndpoint  string       `yaml:"cloud_storage_endpoint,omitempty"`
	Base64Credential      Base64String `yaml:"base64_credential"`
}

func NewDefaultConfig() *Config {
	return &Config{
		Cloud: &Cloud{
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
		},
	}
}

func LoadConfig(path string) (*Config, error) {
	conf := NewDefaultConfig()
	err := goconfig.LoadWithEnv(conf, path)
	if err != nil {
		return nil, err
	}
	if err := conf.Validate(); err != nil {
		return nil, err
	}
	return conf, nil
}

func (c *Config) Validate() error {
	if c.QueueName == "" {
		return errors.New("queue_name is not defined")
	}
	if err := c.Cloud.Validate(); err != nil {
		return errors.Wrap(err, "cloud is invalid")
	}
	if len(c.Rules) == 0 {
		return errors.New("rules is not defined")
	}
	for i, dst := range c.Rules {
		other := c.Rule.Clone()
		dst.MergeIn(other)
		if err := dst.Validate(); err != nil {
			return errors.Wrapf(err, "rule[%d]", i)
		}
		c.Rules[i] = dst
	}
	return nil
}

func (c *Cloud) Validate() error {
	if c == nil {
		return errors.New("not defined")
	}
	if c.AWS == nil {
		return errors.New("aws config is not defined")
	}
	if c.GCP == nil {
		return errors.New("gcp config is not defined")
	}
	return nil
}
