package bqin

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/kayac/bqin/internal/logger"
	"github.com/pkg/errors"
)

const (
	S3URITemplate         = "s3://%s/%s"
	BigQueryTableTemplate = "%s.%s.%s"
)

type Rule struct {
	S3       *S3Soruce            `yaml:"s3"`
	BigQuery *BigQueryDestination `yaml:"big_query"`
	Option   *ImportOption        `yaml:"option"`

	keyMatcher func(string) (bool, []string)
}

type BigQueryDestination struct {
	ProjectID string `yaml:"project_id" json:"project_id"`
	Table     string `yaml:"table" json:"table"`
	Dataset   string `yaml:"dataset" json:"dataset"`
}

type S3Soruce struct {
	Region    string `yaml:"region"`
	Bucket    string `yaml:"bucket"`
	KeyPrefix string `yaml:"key_prefix"`
	KeyRegexp string `yaml:"key_regexp"`
}

type S3Object struct {
	Bucket string `json:"bucket"`
	Object string `json:"object"`
}

func (s S3Object) String() string {
	return fmt.Sprintf(S3URITemplate, s.Bucket, s.Object)
}

type ImportRequestRecord struct {
	Source *S3Object            `json:"source"`
	Target *BigQueryDestination `json:"target"`
	Option *ImportOption        `json:"option"`
}

func (r *Rule) Validate() error {
	if r.BigQuery.ProjectID == "" {
		return errors.New("rule.bigquery.project_id is not defined")
	}
	if r.BigQuery.Dataset == "" {
		return errors.New("rule.bigquery.dataset is not defined")
	}
	if r.BigQuery.Table == "" {
		return errors.New("rule.bigquery.table is not defined")
	}
	if err := r.Option.Validate(); err != nil {
		return errors.Wrap(err, "rule.option")
	}
	return r.buildKeyMacher()
}

func (r *Rule) buildKeyMacher() error {
	switch {
	case r.S3.KeyPrefix != "":
		r.keyMatcher = func(key string) (bool, []string) {
			if strings.HasPrefix(key, r.S3.KeyPrefix) {
				return true, []string{key}
			}
			return false, nil
		}
	case r.S3.KeyRegexp != "":
		reg, err := regexp.Compile(r.S3.KeyRegexp)
		if err != nil {
			return errors.Wrap(err, "rule.s3.key_regexp is invalid")
		}
		r.keyMatcher = func(key string) (bool, []string) {
			capture := reg.FindStringSubmatch(key)
			if len(capture) == 0 {
				return false, nil
			}
			return true, capture
		}
	default:
		return errors.New("rule.s3.key_prefix or key_regexp is not defined")
	}
	return nil
}

//Match must after Valicate
func (r *Rule) match(bucket, key string) (bool, []string) {
	logger.Debugf("try match `s3://%s/%s` to `%s`", bucket, key, r.String())
	if bucket != r.S3.Bucket {
		return false, nil
	}
	return r.keyMatcher(key)
}

func (r *Rule) Match(bucket, key string) (bool, *ImportRequestRecord) {
	ok, capture := r.match(bucket, key)
	if !ok {
		return false, nil
	}
	return true, r.buildImportRequestRecord(bucket, key, capture)
}

//MatchEventRecord must after Valicate
func (r *Rule) MatchEventRecord(record events.S3EventRecord) (bool, *ImportRequestRecord) {
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

func (r *Rule) buildImportRequestRecord(bucket, key string, capture []string) *ImportRequestRecord {
	option := r.Option.Clone()
	option.TemporaryBucket = expandPlaceHolder(option.TemporaryBucket, capture)
	return &ImportRequestRecord{
		Source: &S3Object{
			Bucket: bucket,
			Object: key,
		},
		Target: &BigQueryDestination{
			ProjectID: expandPlaceHolder(r.BigQuery.ProjectID, capture),
			Dataset:   expandPlaceHolder(r.BigQuery.Dataset, capture),
			Table:     expandPlaceHolder(r.BigQuery.Table, capture),
		},
		Option: option,
	}
}

func (r *Rule) Clone() *Rule {
	ret := &Rule{}
	ret.MergeIn(r)
	return ret
}

func (r *Rule) MergeIn(other *Rule) {
	if other == nil {
		return
	}

	if r.S3 == nil {
		r.S3 = other.S3.Clone()
	} else {
		r.S3.MergeIn(other.S3)
	}

	if r.BigQuery == nil {
		r.BigQuery = other.BigQuery.Clone()
	} else {
		r.BigQuery.MergeIn(other.BigQuery)
	}

	if r.Option == nil {
		r.Option = other.Option.Clone()
	} else {
		r.Option.MergeIn(other.Option)
	}
}

func (s3 S3Soruce) String() string {
	if s3.KeyPrefix != "" {
		return fmt.Sprintf(S3URITemplate, s3.Bucket, s3.KeyPrefix)
	} else {
		return fmt.Sprintf(S3URITemplate, s3.Bucket, s3.KeyRegexp)
	}
}

func (s3 *S3Soruce) Clone() *S3Soruce {
	ret := &S3Soruce{}
	ret.MergeIn(s3)
	return ret
}

func (s3 *S3Soruce) MergeIn(other *S3Soruce) {
	if other == nil {
		return
	}
	if s3.Region == "" {
		s3.Region = other.Region
	}
	if s3.Bucket == "" {
		s3.Bucket = other.Bucket
	}
	if s3.KeyPrefix == "" {
		s3.KeyPrefix = other.KeyPrefix
	}
	if s3.KeyRegexp == "" {
		s3.KeyRegexp = other.KeyRegexp
	}
}

func (bq BigQueryDestination) String() string {
	return fmt.Sprintf(BigQueryTableTemplate, bq.ProjectID, bq.Dataset, bq.Table)
}

func (bq *BigQueryDestination) Clone() *BigQueryDestination {
	ret := &BigQueryDestination{}
	ret.MergeIn(bq)
	return ret
}

func (bq *BigQueryDestination) MergeIn(other *BigQueryDestination) {
	if other == nil {
		return
	}
	if bq.ProjectID == "" {
		bq.ProjectID = other.ProjectID
	}
	if bq.Dataset == "" {
		bq.Dataset = other.Dataset
	}
	if bq.Table == "" {
		bq.Table = other.Table
	}
}
