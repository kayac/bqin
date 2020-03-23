package bqin_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/kayac/bqin"
	"github.com/kayac/bqin/cloud"
	"github.com/kayac/bqin/internal/logger"
	"github.com/kayac/bqin/internal/stub"
)

func TestTransporter(t *testing.T) {
	logger.Setup(logger.NewTestingLogWriter(t), GetTestLogLevel())
	stubGCS := stub.NewStubGCS()
	defer stubGCS.Close()
	stubS3 := stub.NewStubS3("testdata/s3/")
	defer stubS3.Close()

	conf := &cloud.Config{
		AWS: &cloud.AWS{
			Region:           "local",
			DisableSSL:       true,
			S3ForcePathStyle: true,
			S3Endpoint:       stubS3.Endpoint(),
		},
		GCP: &cloud.GCP{
			WithoutAuthentication: true,
			CloudStorageEndpoint:  stubGCS.Endpoint(),
		},
	}
	sess := conf.AWS.BuildSession()
	opts := conf.GCP.BuildOption(cloud.CloudStorageServiceID)
	transporter := bqin.NewTransporter(sess, opts...)

	cases := []struct {
		Comment string
		Job     *bqin.TransportJob
		IsErr   bool
	}{
		{
			Comment: "success",
			Job: &bqin.TransportJob{
				Source:      MustParseURL("s3://bqin.bucket.test/data/user/snapshot_at=20200210/part-0001.csv"),
				Destination: MustParseURL("gs://temp-bucket/my-object.csv"),
			},
			IsErr: false,
		},
		{
			Comment: "s3 object not found",
			Job: &bqin.TransportJob{
				Source:      MustParseURL("s3://my-bucket/my-object.csv"),
				Destination: MustParseURL("gs://temp-bucket/my-object.csv"),
			},
			IsErr: true,
		},
		{
			Comment: "source scheme invalid",
			Job: &bqin.TransportJob{
				Source:      MustParseURL("file://root/hoge.csv"),
				Destination: MustParseURL("gs://temp-bucket/my-object.csv"),
			},
			IsErr: true,
		},
		{
			Comment: "destination scheme invalid",
			Job: &bqin.TransportJob{
				Source:      MustParseURL("s3://root/hoge.csv"),
				Destination: MustParseURL("s3://temp-bucket/my-object.csv"),
			},
			IsErr: true,
		},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%02d", i), func(t *testing.T) {
			t.Logf("test %s", c.Job)
			t.Log(c.Comment)
			handle, err := transporter.Transport(context.Background(), c.Job)
			t.Logf("err is %v", err)
			if err == nil {
				defer handle.Cleanup(context.Background())
			}
			if (err != nil) != c.IsErr {
				t.Error("unexpected error state")
			}
		})
	}
}
