package bqin_test

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"github.com/kayac/bqin"
	"github.com/kayac/bqin/internal/logger"
	"github.com/kayac/bqin/internal/stub"
	"github.com/kylelemons/godebug/pretty"

	"gopkg.in/yaml.v2"
)

type E2ECase struct {
	CaseName  string              `yaml:"casename,omitempty"`
	Configure string              `yaml:"configure"`
	Messages  []string            `yaml:"messages"`
	Expected  map[string][]string `yaml:"expected"`

	stubMgr *stub.Manager
	msgs    []*sqs.Message
	conf    *bqin.Config
}

var caseSuccessedYaml string = `
- casename: default
  configure: testdata/config/standard.yaml
  messages:
    - testdata/sqs/user.json
  expected:
    bqin-test-gcp.test.user:
    - "gs://bqin-import-tmp/data/user/snapshot_at=20200210/part-0001.csv"

- casename: hive_format_rule
  configure: testdata/config/hive_format.yaml
  messages:
    - testdata/sqs/user.json
  expected:
    bqin-test-gcp.test.user_20200210:
    - "gs://bqin-import-tmp/data/user/snapshot_at=20200210/part-0001.csv"
`

func TestE2ESuccess(t *testing.T) {
	cases := loadCases(t, caseSuccessedYaml)
	for _, c := range cases {
		t.Run(c.String(), c.Run)
	}

}

func loadCases(t *testing.T, caseYaml string) []*E2ECase {
	reader := strings.NewReader(caseYaml)
	decoder := yaml.NewDecoder(reader)
	var cases []*E2ECase
	if err := decoder.Decode(&cases); err != nil {
		t.Fatal(err)
	}
	return cases
}

func (c *E2ECase) GetLogLevel() string {
	debug := os.Getenv("DEBUG")
	if debug == "" {
		return logger.InfoLevel
	}
	return logger.DebugLevel
}

func (c *E2ECase) String() string {
	if c.CaseName == "" {
		return "file:" + c.Configure
	}
	return c.CaseName
}

func (c *E2ECase) Prepare(t *testing.T) {
	logger.Setup(logger.NewTestingLogWriter(t), c.GetLogLevel())
	c.stubMgr = stub.NewManager("testdata/s3/")

	//generate sqs message
	c.msgs = make([]*sqs.Message, 0, len(c.Messages))
	t.Log(c.Messages)
	for _, msg := range c.Messages {
		body, err := ioutil.ReadFile(msg)
		if err != nil {
			t.Fatalf("Prepare failed, load message body %s:", err)
		}
		msgId, _ := uuid.NewRandom()
		c.msgs = append(c.msgs, &sqs.Message{
			Body:          aws.String(string(body)),
			MD5OfBody:     aws.String(fmt.Sprintf("%x", md5.Sum(body))),
			ReceiptHandle: aws.String(newReceiptHandle()),
			MessageId:     aws.String(msgId.String()),
		})
	}
	c.stubMgr.SQS.SetRecivedMessages(c.msgs)

	var err error
	c.conf, err = bqin.LoadConfig(c.Configure)
	if err != nil {
		t.Fatalf("Prepare failed, load configure  %s:", err)
	}
	c.stubMgr.OverwriteConfig(c.conf.Cloud)
}

func (c *E2ECase) Run(t *testing.T) {
	c.Prepare(t)
	defer c.Cleanup(t)
	app, err := bqin.NewApp(c.conf)
	if err != nil {
		t.Fatalf("app initialize failed: %s", err)
	}
	ctx := context.Background()
	for count := 0; count < len(c.msgs); count++ {
		err := app.OneReceiveAndProcess(ctx)
		if err != nil {
			t.Fatalf("unexpected run error: %s", err)
		}
	}
	err = app.OneReceiveAndProcess(ctx)
	if err != bqin.ErrNoRequest {
		t.Errorf("when no more massage: %s", err)
	}
	loaded := c.stubMgr.BigQuery.LoadedData()
	if !reflect.DeepEqual(loaded, c.Expected) {
		t.Errorf("bigquery loaded data status unexpected: %s", pretty.Compare(loaded, c.Expected))
	}
}

func (c *E2ECase) Cleanup(t *testing.T) {
	c.stubMgr.Close()
}

func newReceiptHandle() string {
	runes := make([]byte, 64)

	for i := 0; i < 64; i++ {
		num, _ := rand.Int(rand.Reader, big.NewInt(255))
		runes[i] = byte(num.Int64())
	}

	return base64.RawStdEncoding.EncodeToString(runes)
}
