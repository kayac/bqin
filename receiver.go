package bqin

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/kayac/bqin/cloud"
	"github.com/kayac/bqin/internal/logger"
	"github.com/lestrrat-go/backoff"
)

var (
	ErrMaxRetry = errors.New("max retry count reached")
)

type SQSReceiver struct {
	cloud *cloud.Cloud

	mu        sync.Mutex
	isChecked bool
	rules     []*Rule
	queueURL  string
	queueName string
}

func NewSQSReceiver(conf *Config, c *cloud.Cloud) *SQSReceiver {
	return &SQSReceiver{
		cloud:     c,
		rules:     conf.Rules,
		queueName: conf.QueueName,
	}
}

func (r *SQSReceiver) Receive(ctx context.Context) (*ImportRequest, error) {
	if err := r.check(ctx); err != nil {
		logger.Errorf("Can't pass check. %s", err)
		return nil, err
	}
	msg, err := r.receive(ctx)
	if err != nil {
		return nil, err
	}

	var isFailed bool = true
	req := &ImportRequest{
		ID:            *msg.MessageId,
		ReceiptHandle: *msg.ReceiptHandle,
	}
	defer func() {
		if isFailed {
			logger.Infof("[%s] Aborted message. ReceiptHandle = %s", req.ID, req.ReceiptHandle)
		}
	}()

	event, err := r.parse(msg)
	if err != nil {
		logger.Errorf("[%s]  Can't parse event from Body. %s", req.ID, err)
		return nil, err
	}
	req.Records = r.convert(event)
	isFailed = false
	return req, nil
}

var policy = backoff.NewExponential(
	backoff.WithInterval(500*time.Millisecond), // base interval
	backoff.WithJitterFactor(0.05),             // 5% jitter
	backoff.WithMaxRetries(5),                  // If not specified, default number of retries is 10
)

func (r *SQSReceiver) Complete(_ context.Context, req *ImportRequest) error {
	var completed = false
	defer func() {
		if !completed {
			logger.Infof("[%s] Can't complete message. ReceiptHandle: %s", req.ID, req.ReceiptHandle)
		}
	}()

	_, err := r.cloud.GetSQS().DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(r.queueURL),
		ReceiptHandle: aws.String(req.ReceiptHandle),
	})
	if err != nil {
		logger.Infof("[%s] Can't delete message : %s", req.ID, err)
		b, cancel := policy.Start(context.Background())
		defer cancel()

		for backoff.Continue(b) {
			_, err = r.cloud.GetSQS().DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      aws.String(r.queueURL),
				ReceiptHandle: aws.String(req.ReceiptHandle),
			})
			if err == nil {
				completed = true
				logger.Infof("[%s] Retry completed message.", req.ID)
				return nil
			}
		}
		logger.Errorf("[%s] Max retry count reached. Giving up. last error: %s", req.ID, err)
		return ErrMaxRetry
	}
	completed = true
	logger.Infof("[%s] Completed message.", req.ID)
	return nil
}

func (r *SQSReceiver) check(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.isChecked {
		return nil
	}

	//first check only
	logger.Infof("Connect to SQS: %s", r.queueName)
	res, err := r.cloud.GetSQS().GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(r.queueName),
	})
	if err != nil {
		return err
	}
	r.isChecked = true
	r.queueURL = *res.QueueUrl
	logger.Debugf("QueueURL is %s", r.queueURL)
	return nil
}

func (r *SQSReceiver) receive(ctx context.Context) (*sqs.Message, error) {
	res, err := r.cloud.GetSQS().ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(1),
		QueueUrl:            aws.String(r.queueURL),
	})
	if err != nil {
		return nil, err
	}
	if len(res.Messages) == 0 {
		return nil, ErrNoRequest
	}
	msg := res.Messages[0]
	msgId := *msg.MessageId
	logger.Infof("[%s] Recieved message.", msgId)
	logger.Debugf("[%s] receipt handle: %s", msgId, *msg.ReceiptHandle)
	logger.Debugf("[%s] body: %s", msgId, *msg.Body)
	return msg, nil
}

//parse sqs messge body as s3 event
func (r *SQSReceiver) parse(msg *sqs.Message) (*events.S3Event, error) {

	if msg.Body == nil {
		return nil, errors.New("body is nil")
	}

	dec := json.NewDecoder(strings.NewReader(*msg.Body))
	var event events.S3Event
	err := dec.Decode(&event)
	for i, _ := range event.Records {
		event.Records[i].S3.Object.URLDecodedKey = event.Records[i].S3.Object.Key
		if !strings.Contains(event.Records[i].S3.Object.Key, "%") {
			continue
		}
		if _key, err := url.QueryUnescape(event.Records[i].S3.Object.Key); err == nil {
			event.Records[i].S3.Object.URLDecodedKey = _key
		}
	}

	return &event, err
}

func (r *SQSReceiver) convert(event *events.S3Event) []*ImportRequestRecord {
	ret := make([]*ImportRequestRecord, 0, len(event.Records))
	for _, record := range event.Records {
		for _, rule := range r.rules {
			ok, reqRecord := rule.MatchEventRecord(record)
			if !ok {
				continue
			}
			logger.Debugf("match rule: %s", rule.String())
			ret = append(ret, reqRecord)
		}
	}
	return ret
}
