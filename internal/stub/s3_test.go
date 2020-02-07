package stub_test

import (
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/kayac/bqin/internal/stub"
	"github.com/kylelemons/godebug/pretty"
)

var stubS3TestInputs = [][]string{
	{"bucket1", "object1.txt"},
	{"bucket1", "object2.txt"},
}

var stubS3ExpectedBody = []string{
	"",
	"hoge\n",
}

var stubS3ExpectedLogs = []string{
	`{"svc":"s3","method":"GET","path":"/bucket1/object1.txt","status_code":404}`,
	`{"svc":"s3","method":"GET","path":"/bucket1/object2.txt","status_code":200}`,
}

func TestStubS3(t *testing.T) {
	stubS3 := stub.NewStubS3("testdata/")
	defer stubS3.Close()
	svc := stubS3.NewSvc()
	for i, input := range stubS3TestInputs {
		resp, err := svc.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(input[0]),
			Key:    aws.String(input[1]),
		})
		if stubS3ExpectedBody[i] == "" && err != nil {
			continue
		}
		if err != nil {
			t.Errorf("[case %d] unexpected body. get failed", i)
			continue
		}
		body, _ := ioutil.ReadAll(resp.Body)
		if string(body) != stubS3ExpectedBody[i] {
			t.Errorf("[case %d] unexpected body: %s", i, pretty.Compare(string(body), stubS3ExpectedBody[i]))
		}
	}
	logs := stubS3.GetLogs()
	if !reflect.DeepEqual(logs, stubS3ExpectedLogs) {
		t.Errorf("unexpected logs: %s", pretty.Compare(logs, stubS3ExpectedLogs))
	}
}
