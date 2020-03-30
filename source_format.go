package bqin

import (
	"fmt"

	"cloud.google.com/go/bigquery"
)

type SourceFormat string

const (
	Unknown SourceFormat = ""
	CSV                  = "csv"
	JSON                 = "json"
	Parquet              = "parquet"
)

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
