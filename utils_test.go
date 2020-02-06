package bqin_test

import (
	"io/ioutil"
	"log"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/hashicorp/logutils"
)

//test utils

func loadTestSQSMessage(t *testing.T, target string) *sqs.Message {
	bs, err := ioutil.ReadFile("testdata/sqs_msg/" + target + ".json")
	if err != nil {
		t.Fatalf("sqs test message `%s` load failed: %s", target, err.Error())
	}
	return &sqs.Message{
		MessageId:     aws.String(target),
		Body:          aws.String(string(bs)),
		ReceiptHandle: aws.String(target),
	}
}

type testWriter struct {
	t *testing.T
}

func (tw *testWriter) Write(bs []byte) (n int, err error) {
	tw.t.Log(string(bs))
	return len(bs), nil
}

func setTestLogger(t *testing.T, level string) {
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"debug", "info", "error"},
		MinLevel: logutils.LogLevel(level),
		Writer:   &testWriter{t: t},
	}
	log.SetOutput(filter)
}
