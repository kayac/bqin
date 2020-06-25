package bqin_test

import (
	"reflect"
	"sort"
	"testing"
	"net/url"

	"github.com/kayac/bqin"
	"github.com/kayac/bqin/internal/logger"
)

func TestResolver(t *testing.T) {
	logger.Setup(logger.NewTestingLogWriter(t), GetLogLevel())

	conf, err := bqin.LoadConfig("testdata/config/default.yaml")
	if err != nil {
		t.Fatalf("Prepare failed, load configure  %s:", err)
	}
	factory := &bqin.Factory{Config: conf}
	resolver := factory.NewResolver()

	jobs := resolver.Resolve([]*url.URL{
		MustParseURL("s3://dummy/dummy.txt"),
		MustParseURL("s3://bqin.bucket.test/dummy.txt"),
		MustParseURL("s3://bqin.bucket.test/data/user.txt"),
		MustParseURL("s3://bqin.bucket.test/data/hoge/part-0001.csv"),
		MustParseURL("s3://bqin.bucket.test/data/hoge/xxxx.txt"),
	})
	actual := make([]string, 0, len(jobs))
	for _, j := range jobs {
		actual = append(actual, j.String())
	}
	sort.Slice(actual, func(i,j int) bool { return actual[i] < actual[j]})
	expected := []string {
		"transport from s3://bqin.bucket.test/data/hoge/part-0001.csv to gs://bqin-import-tmp/data/hoge/part-0001.csv, and load to bqin-test-gcp.test.hoge_0001",
		"transport from s3://bqin.bucket.test/data/user.txt to gs://bqin-import-tmp/data/user.txt, and load to bqin-test-gcp.test.user",
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Logf("actual:   %v", actual)
		t.Logf("expected: %v", expected)
		t.Error("unexpected job strings")
	}
}

