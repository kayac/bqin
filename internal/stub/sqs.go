package stub

import (
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"github.com/kayac/bqin/internal/logger"
)

type StubSQS struct {
	stub
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
	bodyBs, _ := ioutil.ReadFile("testdata/sqs_msg/s3_object_created_put.json")
	io.WriteString(
		w,
		fmt.Sprintf(
			stubSQSReceiveMessageResponseTmpl,
			s.receiptHandle,
			md5.Sum(bodyBs),
			string(bodyBs),
		),
	)
}
func (s *StubSQS) serveDeleteMessage(w http.ResponseWriter, r *http.Request, params url.Values) {
	handle := params.Get("ReceiptHandle")
	if handle != s.receiptHandle {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "ReceiptHandleIsInvalid")
		return
	}
	w.WriteHeader(http.StatusOK)
	io.WriteString(w, stubSQSDeleteMessageResponseTmpl)
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
  <ReceiveMessageResult>
    <Message>
      <MessageId>5fea7756-0ea4-451a-a703-a558b933e274</MessageId>
      <ReceiptHandle>%s</ReceiptHandle>
      <MD5OfBody>%x</MD5OfBody>
      <Body>%s</Body>
      <Attribute>
        <Name>SenderId</Name>
        <Value>195004372649</Value>
      </Attribute>
      <Attribute>
        <Name>SentTimestamp</Name>
        <Value>1238099229000</Value>
      </Attribute>
      <Attribute>
        <Name>ApproximateReceiveCount</Name>
        <Value>5</Value>
      </Attribute>
      <Attribute>
        <Name>ApproximateFirstReceiveTimestamp</Name>
        <Value>1250700979248</Value>
      </Attribute>
    </Message>
  </ReceiveMessageResult>
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
