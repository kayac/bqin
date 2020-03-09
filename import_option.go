package bqin

import (
	"errors"

	"cloud.google.com/go/bigquery"
)

type ImportOption struct {
	TemporaryBucket string `yaml:"temporary_bucket" json:"temporary_bucket"`
	GZip            *bool  `yaml:"gzip,omitempty" json:"gzip,omitempty"`
}

func (o *ImportOption) Validate() error {
	if o == nil {
		return errors.New("not defined")
	}
	if o.TemporaryBucket == "" {
		return errors.New("temporary_bucket is not defined")
	}
	return nil
}

func (o *ImportOption) Clone() *ImportOption {
	ret := &ImportOption{}
	ret.MergeIn(o)
	return ret
}

func (o *ImportOption) MergeIn(other *ImportOption) {
	if other == nil {
		return
	}
	if o.TemporaryBucket == "" {
		o.TemporaryBucket = other.TemporaryBucket
	}
	if o.GZip == nil {
		o.GZip = other.GZip
	}

}

func (o *ImportOption) getCompression() bigquery.Compression {
	if o.GZip == nil {
		return bigquery.None
	}
	if *o.GZip {
		return bigquery.Gzip
	}
	return bigquery.None
}
