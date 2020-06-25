package bqin_test

import (
	"context"
	"testing"

	"github.com/kayac/bqin"
	"github.com/kayac/bqin/internal/logger"
	"github.com/kayac/bqin/internal/stub"
)

func TestReceiver(t *testing.T) {
	logger.Setup(logger.NewTestingLogWriter(t), GetLogLevel())
	stubSQS := stub.NewStubSQS()
	defer stubSQS.Close()

	conf := bqin.NewDefaultConfig()
	conf.Cloud.AWS = &bqin.AWS{
		Region:          "local",
		DisableSSL:      true,
		SQSEndpoint:     stubSQS.Endpoint(),
		AccessKeyID:     "AWS_ACCESS_KEY_ID",
		SecretAccessKey: "AWS_SECRET_ACCESS_KEY",
	}
	factory := &bqin.Factory{Config: conf}
	receiver := factory.NewReceiver()

	t.Run("no messages", func(t *testing.T) {
		stubSQS.ClearMetrix()
		urls, handle, err := receiver.Receive(context.Background())
		if err == nil {
			t.Fatal("unexpected error nil")
		}
		if err.Error() != "no sqs message" {
			t.Fatalf("unexpected error message: %s", err.Error())
		}
		if len(urls) != 0 {
			t.Errorf("unexpected url count: %d", len(urls))
		}
		if handle != nil {
			t.Fatal("unexpected handle status: expected handle is nil")
		}
	})
	t.Run("with complated", func(t *testing.T) {
		stubSQS.ClearMetrix()
		stubSQS.SendMessagesFromFile([]string{"testdata/sqs/user.json"})
		urls, handle, err := receiver.Receive(context.Background())
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if len(urls) != 1 {
			t.Errorf("unexpected url count: %d", len(urls))
		}
		if urls[0].String() != "s3://bqin.bucket.test/data/user/snapshot_at=20200210/part-0001.csv" {
			t.Errorf("unexpected url: %s", urls[0])
		}
		handle.Complete()
		handle.Cleanup()
		if stubSQS.NumberOfMessagesDeleted != stubSQS.NumberOfMessagesReceived {
			t.Errorf("unexpected metrix: Deleted=%d, Received=%d", stubSQS.NumberOfMessagesDeleted, stubSQS.NumberOfMessagesDeleted)
		}
	})
	t.Run("not complated", func(t *testing.T) {
		stubSQS.ClearMetrix()
		stubSQS.SendMessagesFromFile([]string{"testdata/sqs/user.json"})
		urls, handle, err := receiver.Receive(context.Background())
		if err != nil {
			t.Errorf("unexpected error: %s", err)
		}
		if len(urls) != 1 {
			t.Errorf("unexpected url count: %d", len(urls))
		}
		if urls[0].String() != "s3://bqin.bucket.test/data/user/snapshot_at=20200210/part-0001.csv" {
			t.Errorf("unexpected url: %s", urls[0])
		}
		handle.Cleanup()
		if stubSQS.NumberOfMessagesDeleted == stubSQS.NumberOfMessagesReceived {
			t.Errorf("unexpected metrix: Deleted=%d, Received=%d", stubSQS.NumberOfMessagesDeleted, stubSQS.NumberOfMessagesDeleted)
		}
	})
}
