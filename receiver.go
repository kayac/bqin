package bqin

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/lestrrat-go/backoff"
	"github.com/pkg/errors"
)

type SQSReceiver struct {
	sqs    sqsiface.SQSAPI
	finish bool

	mu        sync.Mutex
	isChecked bool
	rules     []*Rule
	queueURL  string
	queueName string
}

func NewSQSReceiver(sess *session.Session, conf *Config) (*SQSReceiver, error) {
	return newSQSReceiver(conf, sqs.New(sess))
}

func newSQSReceiver(conf *Config, svc *sqs.SQS) (*SQSReceiver, error) {
	rules, err := conf.GetMergedRules()
	if err != nil {
		return nil, err
	}
	return &SQSReceiver{
		sqs:       svc,
		rules:     rules,
		queueName: conf.QueueName,
	}, nil
}

func (r *SQSReceiver) Receive(ctx context.Context) (*ImportRequest, error) {
	if err := r.check(ctx); err != nil {
		errorf("Can't pass check. %s", err)
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
			infof("[%s] Aborted message. ReceiptHandle = %s", req.ID, req.ReceiptHandle)
		}
	}()

	event, err := r.parse(msg)
	if err != nil {
		errorf("[%s]  Can't parse event from Body. %s", req.ID, err)
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
			infof("[%s] Can't complete message. ReceiptHandle: %s", req.ID, req.ReceiptHandle)
		}
	}()

	_, err := r.sqs.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(r.queueURL),
		ReceiptHandle: aws.String(req.ReceiptHandle),
	})
	if err != nil {
		infof("[%s] Can't delete message : %s", req.ID, err)
		b, cancel := policy.Start(context.Background())
		defer cancel()

		for backoff.Continue(b) {
			_, err = r.sqs.DeleteMessage(&sqs.DeleteMessageInput{
				QueueUrl:      aws.String(r.queueURL),
				ReceiptHandle: aws.String(req.ReceiptHandle),
			})
			if err == nil {
				completed = true
				infof("[%s] Retry completed message.", req.ID)
				return nil
			}
		}
		errorf("[%s] Max retry count reached. Giving up.", req.ID)
		return errors.Wrap(err, "Max retry count")
	}
	completed = true
	infof("[%s] Completed message.", req.ID)
	return nil
}

func (r *SQSReceiver) check(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.isChecked {
		return nil
	}

	//first check only
	infof("Connect to SQS: %s", r.queueName)
	res, err := r.sqs.GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(r.queueName),
	})
	if err != nil {
		return err
	}
	r.isChecked = true
	r.queueURL = *res.QueueUrl
	debugf("QueueURL is %s", r.queueURL)
	return nil
}

func (r *SQSReceiver) receive(ctx context.Context) (*sqs.Message, error) {
	res, err := r.sqs.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
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
	infof("[%s] Recieved message.", msgId)
	debugf("[%s] receipt handle: %s", msgId, *msg.ReceiptHandle)
	debugf("[%s] body: %s", msgId, *msg.Body)
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
	for _, record := range event.Records {
		if !strings.Contains(record.S3.Object.Key, "%") {
			continue
		}
		if _key, err := url.QueryUnescape(record.S3.Object.Key); err == nil {
			record.S3.Object.URLDecodedKey = _key
		}
	}

	return &event, err
}

func (r *SQSReceiver) convert(event *events.S3Event) []*ImportRequestRecord {
	ret := make([]*ImportRequestRecord, 0, len(event.Records))
	for _, record := range event.Records {
		for _, rule := range r.rules {
			ok, capture := rule.MatchEventRecord(record)
			if !ok {
				continue
			}
			debugf("match rule: %s", rule.String())
			ret = append(ret, &ImportRequestRecord{
				Source: &ImportSource{
					Bucket: record.S3.Bucket.Name,
					Object: record.S3.Object.Key,
				},
				Target: rule.BuildImportTarget(capture),
			})
		}
	}
	return ret
}
