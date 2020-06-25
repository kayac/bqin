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
	"github.com/kayac/bqin/internal/logger"
	"github.com/lestrrat-go/backoff"
	"github.com/pkg/errors"
)

type Receiver struct {
	//for sqs client session
	sess *session.Session

	mu        sync.Mutex
	queueName string
	queueURL  string
}

func NewReceiver(queueName string, sess *session.Session) *Receiver {
	return &Receiver{
		sess:      sess,
		queueName: queueName,
	}
}

type ReceiptHandle struct {
	sess             *session.Session
	queueURL         string
	isCompelete      bool
	msgId            string
	msgReceiptHandle string
}

func (r *Receiver) Receive(ctx context.Context) ([]*url.URL, *ReceiptHandle, error) {
	qurl, err := r.getQueueURL()
	if err != nil {
		return nil, nil, err
	}
	svc := sqs.New(r.sess)
	res, err := svc.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(1),
		QueueUrl:            aws.String(qurl),
	})
	if err != nil {
		return nil, nil, err
	}
	if len(res.Messages) == 0 {
		return nil, nil, ErrNoMessage
	}
	msg := res.Messages[0]
	handle := newReceiptHandle(r.sess, qurl, msg)
	handle.Debugf("body: %s", *msg.Body)

	if msg.Body == nil {
		return nil, handle, errors.New("body is nil")
	}
	dec := json.NewDecoder(strings.NewReader(*msg.Body))
	var event events.S3Event
	if err := dec.Decode(&event); err != nil {
		return nil, handle, errors.Wrap(err, "body parse failed")
	}

	urls := make([]*url.URL, 0, len(event.Records))
	for _, record := range event.Records {
		record.S3.Object.URLDecodedKey = record.S3.Object.Key
		if strings.Contains(record.S3.Object.Key, "%") {
			if decordedKey, err := url.QueryUnescape(record.S3.Object.Key); err == nil {
				record.S3.Object.URLDecodedKey = decordedKey
			}
		}
		u := &url.URL{
			Scheme: "s3",
			Host:   record.S3.Bucket.Name,
			Path:   record.S3.Object.URLDecodedKey,
		}
		handle.Debugf("message include %s", u.String())
		urls = append(urls, u)
	}
	return urls, handle, nil
}

func (r *Receiver) getQueueURL() (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.queueURL != "" {
		return r.queueURL, nil
	}

	ctx := context.Background()
	logger.Infof("Check sqs name:%s", r.queueName)
	res, err := sqs.New(r.sess).GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(r.queueName),
	})
	if err != nil {
		return "", errors.Wrap(err, "cannot get sqs queue url")
	}
	r.queueURL = *res.QueueUrl
	logger.Debugf("QueueURL is %s", r.queueURL)
	return r.queueURL, nil
}

func (r *Receiver) SetQueueName(queueName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.queueName == queueName {
		return
	}
	r.queueName = queueName
	r.queueURL = ""
}

func (r *Receiver) GetQueueName() string {
	return r.queueName
}

func newReceiptHandle(sess *session.Session, queueURL string, msg *sqs.Message) *ReceiptHandle {
	handle := &ReceiptHandle{
		sess:             sess,
		isCompelete:      false,
		queueURL:         queueURL,
		msgId:            *msg.MessageId,
		msgReceiptHandle: *msg.ReceiptHandle,
	}
	handle.Infof("Recieved message.")
	handle.Debugf("receipt handle: %s", handle.msgReceiptHandle)
	return handle
}

func (h *ReceiptHandle) Infof(format string, args ...interface{}) {
	args = append([]interface{}{h.msgId}, args...)
	logger.Infof("[%s]"+format, args...)
}

func (h *ReceiptHandle) Debugf(format string, args ...interface{}) {
	args = append([]interface{}{h.msgId}, args...)
	logger.Debugf("[%s]"+format, args...)
}

func (h *ReceiptHandle) Errorf(format string, args ...interface{}) {
	args = append([]interface{}{h.msgId}, args...)
	logger.Errorf("[%s]"+format, args...)
}

var policy = backoff.NewExponential(
	backoff.WithInterval(500*time.Millisecond), // base interval
	backoff.WithJitterFactor(0.05),             // 5% jitter
	backoff.WithMaxRetries(5),                  // If not specified, default number of retries is 10
)

func (h *ReceiptHandle) Complete() error {
	if h == nil {
		return nil
	}

	if h.isCompelete {
		return nil
	}

	input := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(h.queueURL),
		ReceiptHandle: aws.String(h.msgReceiptHandle),
	}
	h.Debugf("input is %#v", input)
	svc := sqs.New(h.sess)
	_, err := svc.DeleteMessage(input)
	if err == nil {
		h.isCompelete = true
		h.Infof("Completed message.")
		return nil
	}

	h.Infof("Can't delete message (retry count = 0): %s", err)
	b, cancel := policy.Start(context.Background())
	defer cancel()

	for i := 1; backoff.Continue(b); i++ {
		_, err = svc.DeleteMessage(input)
		if err == nil {
			h.isCompelete = true
			h.Infof("Retry completed message.")
			return nil
		}
		h.Infof("Can't delete message (retry count = %d): %s", i, err)
	}
	h.Infof("Can't delete message. ReceiptHandle: %s", h.msgReceiptHandle)
	h.Errorf("Max retry count reached. Giving up. last error: %s", err)
	return ErrMaxRetry
}

func (h *ReceiptHandle) Cleanup() {
	if h != nil && !h.isCompelete {
		h.Infof("This message not completed, ReceiptHandle: %s", h.msgReceiptHandle)
	}
}
