package bqin

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/kayac/bqin/cloud"
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
	Cloud              *cloud.Config        `yaml:"cloud"`
	S3                 *S3Soruce            `yaml:"s3"`
	BigQuery           *BigQueryDestination `yaml:"big_query"`
	Rules              []*Rule              `yaml:"rules"`
}

type Rule struct {
	S3         *S3Soruce            `yaml:"s3"`
	BigQuery   *BigQueryDestination `yaml:"big_query"`
	keyMatcher func(string) (bool, []string)
}

type BigQueryDestination struct {
	ProjectID string `yaml:"project_id"`
	Table     string `yaml:"table"`
	Dataset   string `yaml:"dataset"`
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

func LoadConfig(path string) (*Config, error) {
	conf := &Config{
		Cloud: cloud.NewDefaultConfig(),
	}
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
		if ret.BigQuery.ProjectID == "" {
			ret.BigQuery.ProjectID = defaultRule.BigQuery.ProjectID
		}
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
		r.keyMatcher = func(key string) (bool, []string) {
			if strings.HasPrefix(key, prefix) {
				return true, []string{key}
			}
			return false, nil
		}
		return nil
	}
	if regStr := r.S3.KeyRegexp; regStr != "" {
		reg, err := regexp.Compile(regStr)
		if err != nil {
			return err
		}
		r.keyMatcher = func(key string) (bool, []string) {
			capture := reg.FindStringSubmatch(key)
			if len(capture) == 0 {
				return false, nil
			}
			return true, capture
		}
		return nil
	}
	return errors.New("rule.s3.key_prefix or key_regexp is not defined")
}

func (r *Rule) Match(bucket, key string) (bool, []string) {
	if bucket != r.S3.Bucket {
		return false, nil
	}
	return r.keyMatcher(key)
}

func (r *Rule) MatchEventRecord(record events.S3EventRecord) (bool, []string) {
	if record.S3.Object.URLDecodedKey != "" {
		return r.Match(record.S3.Bucket.Name, record.S3.Object.URLDecodedKey)
	}
	return r.Match(record.S3.Bucket.Name, record.S3.Object.Key)
}

func (r *Rule) String() string {
	return strings.Join([]string{r.S3.String(), r.BigQuery.String()}, " => ")
}

// example: when capture []string{"hoge"},  table_$1 => table_hoge
func expandPlaceHolder(s string, capture []string) string {
	for i, v := range capture {
		s = strings.Replace(s, "$"+strconv.Itoa(i), v, -1)
	}
	return s
}

func (r *Rule) BuildImportTarget(capture []string) *ImportTarget {
	return &ImportTarget{
		ProjectID: expandPlaceHolder(r.BigQuery.ProjectID, capture),
		Dataset:   expandPlaceHolder(r.BigQuery.Dataset, capture),
		Table:     expandPlaceHolder(r.BigQuery.Table, capture),
	}
}
