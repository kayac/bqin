package bqin

import "errors"

var (
	ErrMaxRetry  = errors.New("max retry count reached")
	ErrNoMessage = errors.New("no sqs message")
)
