package bqin

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	goconfig "github.com/kayac/go-config"
	"github.com/pkg/errors"
)

const (
	S3URITemplate         = "s3://%s/%s"
	BigQueryTableTemplate = "%s.%s"
)

type Config struct {
	QueueName          string               `yaml:"queue_name"`
	GCSTemporaryBucket string               `yaml:"gcs_temporary_bucket"`
	AWS                *AWSConfig           `yaml:"aws"`
	GCP                *GCPConfig           `yaml:"gcp"`
	S3                 *S3Soruce            `yaml:"s3"`
	BigQuery           *BigQueryDestination `yaml:"big_query"`
	Rules              []*Rule              `yaml:"rules"`
}

type AWSConfig struct {
	Region string `yaml:"region"`
}

type GCPConfig struct {
	ProjectID string `yaml:"project_id"`
}

type Rule struct {
	S3         *S3Soruce            `yaml:"s3"`
	BigQuery   *BigQueryDestination `yaml:"big_query"`
	keyMatcher func(string) bool
}

type BigQueryDestination struct {
	Table   string `yaml:"table"`
	Dataset string `yaml:"dataset"`
}

func (bq BigQueryDestination) String() string {
	return fmt.Sprintf(BigQueryTableTemplate, bq.Dataset, bq.Table)
}

type S3Soruce struct {
	Region    string `yaml:"region"`
	Bucket    string `yaml:"bucket"`
	KeyPrefix string `yaml:"key_prefix"`
	KeyRegexp string `yaml:"key_regexp"`
}

func (s3 S3Soruce) String() string {
	if s3.KeyPrefix != "" {
		return fmt.Sprintf(S3URITemplate, s3.Bucket, s3.KeyPrefix)
	} else {
		return fmt.Sprintf(S3URITemplate, s3.Bucket, s3.KeyRegexp)
	}
}

func newDefaultConfig() *Config {
	return &Config{
		AWS: &AWSConfig{
			Region: os.Getenv("AWS_REGION"),
		},
		GCP: &GCPConfig{
			ProjectID: os.Getenv("GCP_PROJECT_ID"),
		},
	}
}

func LoadConfig(path string) (*Config, error) {
	conf := newDefaultConfig()
	err := goconfig.LoadWithEnv(conf, path)
	return conf, err
}

func (c *Config) GetMergedRules() ([]*Rule, error) {
	ret := make([]*Rule, 0, len(c.Rules))
	defaultRule := &Rule{
		S3:       c.S3,
		BigQuery: c.BigQuery,
	}
	for _, r := range c.Rules {
		merged, err := mergeRule(defaultRule, r)
		if err != nil {
			return nil, errors.Wrapf(err, "rule: %s", r)
		}
		ret = append(ret, merged)
	}
	return ret, nil
}

func mergeRule(defaultRule *Rule, baseRule *Rule) (*Rule, error) {
	var ret Rule = *baseRule
	if ret.S3 == nil {
		ret.S3 = defaultRule.S3
	} else {
		if ret.S3.Region == "" {
			ret.S3.Region = defaultRule.S3.Region
		}
		if ret.S3.Bucket == "" {
			ret.S3.Bucket = defaultRule.S3.Bucket
		}
		if ret.S3.KeyPrefix == "" {
			ret.S3.KeyPrefix = defaultRule.S3.KeyPrefix
		}
		if ret.S3.KeyRegexp == "" {
			ret.S3.KeyRegexp = defaultRule.S3.KeyRegexp
		}
	}
	if ret.BigQuery == nil {
		ret.BigQuery = defaultRule.BigQuery
	} else {
		if ret.BigQuery.Dataset == "" {
			ret.BigQuery.Dataset = defaultRule.BigQuery.Dataset
		}
		if ret.BigQuery.Table == "" {
			ret.BigQuery.Table = defaultRule.BigQuery.Table
		}
	}
	err := (&ret).buildKeyMacher()
	return &ret, err
}

func (r *Rule) buildKeyMacher() error {
	if prefix := r.S3.KeyPrefix; prefix != "" {
		r.keyMatcher = func(key string) bool {
			if strings.HasPrefix(key, prefix) {
				return true
			}
			return false
		}
		return nil
	}
	if regStr := r.S3.KeyRegexp; regStr != "" {
		reg, err := regexp.Compile(regStr)
		if err != nil {
			return err
		}
		r.keyMatcher = reg.MatchString
		return nil
	}
	return errors.New("rule.s3.key_prefix or key_regexp is not defined")
}

func (r *Rule) Match(bucket, key string) bool {
	if bucket != r.S3.Bucket {
		return false
	}
	return r.keyMatcher(key)
}

func (r *Rule) MatchEventRecord(record events.S3EventRecord) bool {
	return r.Match(record.S3.Bucket.Name, record.S3.Object.Key)
}

func (r *Rule) String() string {
	return strings.Join([]string{r.S3.String(), r.BigQuery.String()}, " => ")
}
