package stub

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"net/http"
	"net/url"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"github.com/kayac/bqin/internal/logger"
)

type StubSQS struct {
	stub
	msgs          []*sqs.Message
	receiptHandle string
}

func NewStubSQS(receiptHandle string) *StubSQS {
	s := &StubSQS{}
	s.setSvcName("sqs")
	s.receiptHandle = receiptHandle
	r := s.getRouter()
	r.PathPrefix("/").HandlerFunc(s.serveHTTP).Methods("POST")
	return s
}

func NewReceiptHandle() string {
	runes := make([]byte, 64)
	for i := 0; i < 64; i++ {
		num, _ := rand.Int(rand.Reader, big.NewInt(255))
		runes[i] = byte(num.Int64())
	}
	return base64.RawStdEncoding.EncodeToString(runes)
}

func NewSQSMessageFromFile(path string) (*sqs.Message, error) {
	body, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	msgId, _ := uuid.NewRandom()
	msg := &sqs.Message{
		Body:          aws.String(string(body)),
		MD5OfBody:     aws.String(fmt.Sprintf("%x", md5.Sum(body))),
		ReceiptHandle: aws.String(NewReceiptHandle()),
		MessageId:     aws.String(msgId.String()),
	}
	return msg, nil
}

func (s *StubSQS) SendMessagesFromFile(paths []string) error {
	msgs := make([]*sqs.Message, 0, len(paths))
	for _, path := range paths {
		msg, err := NewSQSMessageFromFile(path)
		if err != nil {
			return err
		}
		msgs = append(msgs, msg)
	}
	s.SetRecivedMessages(msgs)
	return nil
}

func (s *StubSQS) SetRecivedMessages(msgs []*sqs.Message) {
	s.msgs = make([]*sqs.Message, 0, len(msgs))
	s.msgs = append(s.msgs, msgs...)
}

func (s *StubSQS) serveHTTP(w http.ResponseWriter, r *http.Request) {
	l := s.getAccessLog(r)
	bs, _ := ioutil.ReadAll(r.Body)
	paramsString := string(bs)
	logger.Debugf("[stub_sqs] Request Body: %s", paramsString)
	params, err := url.ParseQuery(paramsString)
	if err != nil {
		l.SQSAction = "Unkown"
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	switch params.Get("Action") {
	case "GetQueueUrl":
		s.serveGetQueueUrl(w, r, params)
	case "ReceiveMessage":
		s.serveReceiveMessage(w, r, params)
	case "DeleteMessage":
		s.serveDeleteMessage(w, r, params)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

func (s *StubSQS) serveGetQueueUrl(w http.ResponseWriter, r *http.Request, params url.Values) {
	w.WriteHeader(http.StatusOK)
	io.WriteString(
		w,
		fmt.Sprintf(
			stubSQSGetQueueUrlResponseTmpl,
			s.getServer().URL+"/queue/"+params.Get("QueueName"),
		),
	)
}

func (s *StubSQS) serveReceiveMessage(w http.ResponseWriter, r *http.Request, params url.Values) {
	w.WriteHeader(http.StatusOK)
	payload := ""
	if len(s.msgs) > 0 {
		msg := s.msgs[0]
		msgStrct := struct {
			XMLName       xml.Name `xml:"Message"`
			MessageId     string   `xml:"MessageId"`
			ReceiptHandle string   `xml:"ReceiptHandle"`
			Body          string   `xml:"Body"`
			MD5OfBody     string   `xml:"MD5OfBody"`
		}{
			MessageId:     getString(msg.MessageId),
			ReceiptHandle: getString(msg.ReceiptHandle),
			Body:          getString(msg.Body),
			MD5OfBody:     getString(msg.MD5OfBody),
		}

		payloadBs, err := xml.Marshal(msgStrct)
		if err != nil {
			logger.Debugf("[stub_sqs] sqs.Message can not Marshal :%s", err)
		}
		payload = string(payloadBs)
	}
	response := fmt.Sprintf(stubSQSReceiveMessageResponseTmpl, payload)
	io.WriteString(w, response)
}

func (s *StubSQS) serveDeleteMessage(w http.ResponseWriter, r *http.Request, params url.Values) {
	handle := params.Get("ReceiptHandle")
	for i, msg := range s.msgs {
		if handle == getString(msg.ReceiptHandle) {
			s.msgs = append(s.msgs[0:i], s.msgs[i+1:len(s.msgs)]...)
			w.WriteHeader(http.StatusOK)
			io.WriteString(w, stubSQSDeleteMessageResponseTmpl)
			return
		}
	}
	w.WriteHeader(http.StatusBadRequest)
	io.WriteString(w, "ReceiptHandleIsInvalid")
}

const (
	// see https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_GetQueueUrl.html
	stubSQSGetQueueUrlResponseTmpl = `
<GetQueueUrlResponse>
    <GetQueueUrlResult>
        <QueueUrl>%s</QueueUrl>
    </GetQueueUrlResult>
    <ResponseMetadata>
        <RequestId>470a6f13-2ed9-4181-ad8a-2fdea142988e</RequestId>
    </ResponseMetadata>
</GetQueueUrlResponse>
`
	// see https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html
	stubSQSReceiveMessageResponseTmpl = `
<ReceiveMessageResponse>
  <ReceiveMessageResult>%s</ReceiveMessageResult>
  <ResponseMetadata>
    <RequestId>b6633655-283d-45b4-aee4-4e84e0ae6afa</RequestId>
  </ResponseMetadata>
</ReceiveMessageResponse>
`

	// see https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html
	stubSQSDeleteMessageResponseTmpl = `
<DeleteMessageResponse>
    <ResponseMetadata>
        <RequestId>b5293cb5-d306-4a17-9048-b263635abe42</RequestId>
    </ResponseMetadata>
</DeleteMessageResponse>
`
)

func getString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
