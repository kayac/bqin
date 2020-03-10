package bqin

import (
	"errors"
	"fmt"

	"github.com/kayac/bqin/internal/logger"

	"cloud.google.com/go/bigquery"
)

type SourceFormat string

const (
	Unknown SourceFormat = ""
	CSV                  = "csv"
	JSON                 = "json"
	Parquet              = "parquet"
)

type ImportOption struct {
	TemporaryBucket string       `yaml:"temporary_bucket" json:"temporary_bucket"`
	GZip            *bool        `yaml:"gzip,omitempty" json:"gzip,omitempty"`
	AutoDetect      *bool        `yaml:"auto_detect,omitempty" json:"auto_detect,omitempty"`
	SourceFormat    SourceFormat `yaml:"source_format" json:"source_format"`
}

func (o *ImportOption) Validate() error {
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
	if o.AutoDetect == nil {
		o.AutoDetect = other.AutoDetect
	}
	if o.SourceFormat == Unknown || !o.SourceFormat.IsSupport() {
		o.SourceFormat = other.SourceFormat
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

func (o *ImportOption) getAutoDetect() bool {
	if o.AutoDetect == nil {
		return false
	}
	return *o.AutoDetect
}

func (o *ImportOption) getSourceFormat() bigquery.DataFormat {
	return o.SourceFormat.toBigQuery()
}

func (f SourceFormat) Is(others ...SourceFormat) bool {
	for _, o := range others {
		if f == o {
			return true
		}
	}
	return false
}

func (f SourceFormat) IsSupport() bool {
	return f.Is(CSV, JSON, Parquet)
}

func (f SourceFormat) toBigQuery() bigquery.DataFormat {
	switch f {
	case CSV:
		return bigquery.CSV
	case JSON:
		return bigquery.JSON
	case Parquet:
		return bigquery.Parquet
	}
	panic(fmt.Sprintf("source_format[%s] is unsupported.", f))
}
