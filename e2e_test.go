package bqin_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/kayac/bqin"
	"github.com/kayac/bqin/internal/logger"
	"github.com/kylelemons/godebug/pretty"
)

func TestE2E(t *testing.T) {
	cases := []struct {
		CaseName  string
		Configure string
		Messages  []string
		Expected  map[string][]string
	}{
		{
			CaseName:  "default",
			Configure: "testdata/config/standard.yaml",
			Messages: []string{
				"testdata/sqs/user.json",
			},
			Expected: map[string][]string{
				"bqin-test-gcp.test.user": []string{
					"gs://bqin-import-tmp/data/user/snapshot_at=20200210/part-0001.csv",
				},
			},
		},
		{
			CaseName:  "hive_format_rule",
			Configure: "testdata/config/hive_format.yaml",
			Messages: []string{
				"testdata/sqs/user.json",
			},
			Expected: map[string][]string{
				"bqin-test-gcp.test.user_20200210": []string{
					"gs://bqin-import-tmp/data/user/snapshot_at=20200210/part-0001.csv",
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.CaseName, func(t *testing.T) {
			logger.Setup(logger.NewTestingLogWriter(t), GetLogLevel())
			mgr := NewStubManager("testdata/s3/")
			defer mgr.Close()
			t.Log(c.Messages)
			if err := mgr.SQS.SendMessagesFromFile(c.Messages); err != nil {
				t.Fatalf("Prepare failed, load message body %s:", err)
			}

			conf, err := bqin.LoadConfig(c.Configure)
			if err != nil {
				t.Fatalf("Prepare failed, load configure  %s:", err)
			}
			mgr.OverwriteConfig(conf)
			app := bqin.NewApp(conf)

			ctx := context.Background()
			for count := 0; count < len(c.Messages); count++ {
				err := app.OneReceiveAndProcess(ctx)
				if err != nil {
					t.Fatalf("unexpected run error: %s", err)
				}
			}
			if err := app.OneReceiveAndProcess(ctx); err != bqin.ErrNoMessage {
				t.Errorf("when no more massage: %s", err)
			}
			loaded := mgr.BigQuery.LoadedData()
			if !reflect.DeepEqual(loaded, c.Expected) {
				t.Errorf("bigquery loaded data status unexpected: %s", pretty.Compare(loaded, c.Expected))
			}
		})
	}

}
