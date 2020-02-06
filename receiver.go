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

	mu            sync.Mutex
	isChecked     bool
	queueURL      string
	queueName     string
	targetDataset string
	targetTable   string
}

func NewSQSReceiver(sess *session.Session, conf *Config) *SQSReceiver {
	return newSQSReceiver(conf, sqs.New(sess))
}

func newSQSReceiver(conf *Config, svc *sqs.SQS) *SQSReceiver {
	return &SQSReceiver{
		sqs:           svc,
		targetTable:   conf.GCP.TargetTable,
		targetDataset: conf.GCP.TargetDataset,
		queueName:     conf.AWS.Queue,
	}
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
	msgId := *msg.MessageId
	defer func() {
		if isFailed {
			infof("[%s] Aborted message. ReceiptHandle = %s", msgId, *msg.ReceiptHandle)
		}
	}()

	event, err := r.parse(msg)
	if err != nil {
		errorf("[%s]  Can't parse event from Body. %s", msgId, err)
		return nil, err
	}
	records := r.convert(event)
	isFailed = false
	return &ImportRequest{
		ID:            msgId,
		ReceiptHandle: *msg.ReceiptHandle,
		Records:       records,
	}, nil
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
	debugf("[%s] handle: %s", msgId, *msg.ReceiptHandle)
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

func (r *SQSReceiver) convert(msg *events.S3Event) []ImportRequestRecord {
	ret := make([]ImportRequestRecord, 0, len(msg.Records))
	for _, record := range msg.Records {
		ret = append(ret, ImportRequestRecord{
			SourceBucketName: record.S3.Bucket.Name,
			SourceObjectKey:  record.S3.Object.Key,
			TargetDataset:    r.targetDataset,
			TargetTable:      r.targetTable,
		})
	}
	return ret
}
