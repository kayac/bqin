package bqin_test

import (
	"context"
	"testing"

	"github.com/kayac/bqin"
	"github.com/kayac/bqin/internal/logger"
	"github.com/pkg/errors"
)

func TestProcess(t *testing.T) {

	conf, err := bqin.LoadConfig("testdata/config/default.yaml")
	if err != nil {
		t.Fatalf("test config can not load: %v", err)
	}
	cases := []struct {
		casename string
		req      *bqin.ImportRequest
		isErr    bool
	}{
		{
			casename: "success",
			req: &bqin.ImportRequest{
				ReceiptHandle: "123",
				Records: []*bqin.ImportRequestRecord{
					{
						Source: &bqin.ImportSource{
							Bucket: "bqin.bucket.test",
							Object: "data/user/part-0001.csv",
						},
						Target: &bqin.ImportTarget{
							Dataset: "test",
							Table:   "user",
						},
					},
				},
			},
			isErr: false,
		},
	}

	for _, c := range cases {
		t.Run(c.casename, func(t *testing.T) {
			logger.Setup(logger.NewTestingLogWriter(t), logger.DebugLevel)
			processor, closer := bqin.NewMockedTransporter(t, conf)
			defer closer()

			err := processor.Process(context.Background(), c.req)
			isErr := err != nil
			if isErr != c.isErr {
				t.Logf("error = %#v", err)
				t.Logf("original error = %#v", errors.Cause(err))
				t.Error("error status is unexpected")
			}
		})
	}
}
