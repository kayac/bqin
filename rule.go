package bqin

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/kayac/bqin/internal/logger"
	"github.com/pkg/errors"
)

const (
	S3URITemplate         = "s3://%s/%s"
	BigQueryTableTemplate = "%s.%s.%s"
)

type Rule struct {
	S3       *S3Soruce           `yaml:"s3"`
	BigQuery *LoadingDestination `yaml:"big_query"`
	Option   *JobOption          `yaml:"option"`

	keyMatcher func(string) (bool, []string)
}

type LoadingDestination struct {
	ProjectID string `yaml:"project_id" json:"project_id"`
	Dataset   string `yaml:"dataset" json:"dataset"`
	Table     string `yaml:"table" json:"table"`
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
			if strings.HasPrefix(strings.Trim(key, "/"), r.S3.KeyPrefix) {
				return true, []string{key}
			}
			logger.Debugf("object key start %s.key is %s`", r.S3.KeyPrefix, key)
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
				logger.Debugf("object key not match regexp(%s). key is %s`", r.S3.KeyRegexp, key)
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
		logger.Debugf("bucket name is missmatch: %s is not %s`", bucket, r.S3.Bucket)
		return false, nil
	}
	return r.keyMatcher(key)
}

func (r *Rule) Match(u *url.URL) (bool, []string) {
	if u.Scheme != "s3" {
		return false, nil
	}
	return r.match(u.Host, strings.TrimPrefix(u.Path, "/"))
}

func (r *Rule) String() string {
	return strings.Join([]string{r.S3.String(), r.BigQuery.String()}, " => ")
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

func (bq LoadingDestination) String() string {
	return fmt.Sprintf(BigQueryTableTemplate, bq.ProjectID, bq.Dataset, bq.Table)
}

func (bq *LoadingDestination) Clone() *LoadingDestination {
	ret := &LoadingDestination{}
	ret.MergeIn(bq)
	return ret
}

func (bq *LoadingDestination) MergeIn(other *LoadingDestination) {
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

type JobOption struct {
	TemporaryBucket string       `yaml:"temporary_bucket" json:"temporary_bucket"`
	GZip            *bool        `yaml:"gzip,omitempty" json:"gzip,omitempty"`
	AutoDetect      *bool        `yaml:"auto_detect,omitempty" json:"auto_detect,omitempty"`
	SourceFormat    SourceFormat `yaml:"source_format" json:"source_format"`
}

func (o *JobOption) Validate() error {
	if o == nil {
		return errors.New("not defined")
	}
	if o.TemporaryBucket == "" {
		return errors.New("temporary_bucket is not defined")
	}
	if !o.SourceFormat.IsSupport() {
		return errors.New("source_format is not supported")
	}
	if o.getAutoDetect() && !o.SourceFormat.Is(CSV, JSON) {
		logger.Infof("auto_detect works only when source_format is csv or json")
	}
	return nil
}

func (o *JobOption) Clone() *JobOption {
	ret := &JobOption{}
	ret.MergeIn(o)
	return ret
}

func (o *JobOption) MergeIn(other *JobOption) {
	if other == nil {
		return
	}
	if o.TemporaryBucket == "" {
		o.TemporaryBucket = other.TemporaryBucket
	}
	if o.GZip == nil {
		o.GZip = other.GZip
	}
	if o.AutoDetect == nil {
		o.AutoDetect = other.AutoDetect
	}
	if o.SourceFormat == Unknown || !o.SourceFormat.IsSupport() {
		o.SourceFormat = other.SourceFormat
	}

}

func (o *JobOption) getCompression() bigquery.Compression {
	if o.GZip == nil {
		return bigquery.None
	}
	if *o.GZip {
		return bigquery.Gzip
	}
	return bigquery.None
}

func (o *JobOption) getAutoDetect() bool {
	if o.AutoDetect == nil {
		return false
	}
	return *o.AutoDetect
}

func (o *JobOption) getSourceFormat() bigquery.DataFormat {
	return o.SourceFormat.toBigQuery()
}
