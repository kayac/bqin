package bqin_test

import (
	"os"
	"testing"

	"github.com/kayac/bqin"
)

var ExpectedDefault = []string{
	"s3://bqin.bucket.test/data/user => test.user",
	"s3://bqin.bucket.test/data/(.+)/part-([0-9]+).csv => test.$1_$2",
}

func TestLoadConfig(t *testing.T) {
	os.Setenv("AWS_REGION", "ap-northeast-1")
	os.Setenv("GCP_PROJECT_ID", "bqin-test-gcp")
	testConfig(t, "testdata/config/default.yaml", ExpectedDefault)
}

func testConfig(t *testing.T, path string, expected []string) {
	t.Run(path, func(t *testing.T) {
		conf, err := bqin.LoadConfig(path)
		if err != nil {
			t.Fatalf("load config failed: %s", err)
		}
		merged, err := conf.GetMergedRules()
		if err != nil {
			t.Fatalf("get mergetd rules failed: %s", err)
		}
		if len(merged) != len(expected) {
			t.Error("expected rule num missmatch")
			return
		}
		for i, rule := range merged {
			if rule.String() != expected[i] {
				t.Errorf("Rule[%d] name unexpected :%s", i, rule.String())
			}
		}
	})
}
