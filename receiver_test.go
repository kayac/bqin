package bqin_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/kayac/bqin"
	"github.com/kayac/bqin/internal/logger"
	"github.com/kylelemons/godebug/pretty"
	"github.com/pkg/errors"
)

func TestReceive(t *testing.T) {
	conf, err := bqin.LoadConfig("testdata/config/default.yaml")
	if err != nil {
		t.Fatalf("test config can not load: %v", err)
	}
	cases := []struct {
		casename string
		expected *bqin.ImportRequest
		isErr    bool
	}{
		{
			casename: "success",
			expected: &bqin.ImportRequest{
				ID:            "5fea7756-0ea4-451a-a703-a558b933e274",
				ReceiptHandle: bqin.StubReceiptHandle,
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
					{
						Source: &bqin.ImportSource{
							Bucket: "bqin.bucket.test",
							Object: "data/user/part-0001.csv",
						},
						Target: &bqin.ImportTarget{
							Dataset: "test",
							Table:   "user_0001",
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
			receiver, closer := bqin.NewReceiverWithStub(t, conf)
			defer closer()

			request, err := receiver.Receive(context.Background())
			isErr := err != nil
			if isErr != c.isErr {
				t.Logf("error = %#v", err)
				t.Logf("original error = %#v", errors.Cause(err))
				t.Error("error status is unexpected")
			}
			if !reflect.DeepEqual(request, c.expected) {
				t.Logf("request diff: %s", pretty.Compare(request, c.expected))
				t.Error("request unexpected content")
			}
		})
	}
}

func TestComplete(t *testing.T) {
	conf, err := bqin.LoadConfig("testdata/config/default.yaml")
	if err != nil {
		t.Fatalf("test config can not load: %v", err)
	}
	cases := []struct {
		casename string
		request  *bqin.ImportRequest
		isErr    bool
	}{
		{
			casename: "success",
			request: &bqin.ImportRequest{
				ID:            "5fea7756-0ea4-451a-a703-a558b933e274",
				ReceiptHandle: bqin.StubReceiptHandle,
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
			receiver, closer := bqin.NewReceiverWithStub(t, conf)
			defer closer()

			err := receiver.Complete(context.Background(), c.request)
			isErr := err != nil
			if isErr != c.isErr {
				t.Logf("error = %#v", err)
				t.Logf("original error = %#v", errors.Cause(err))
				t.Error("error status is unexpected")
			}
		})
	}
}
