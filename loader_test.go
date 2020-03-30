package bqin_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/kayac/bqin"
	"github.com/kayac/bqin/internal/logger"
	"github.com/kayac/bqin/internal/stub"
	"google.golang.org/api/option"
)

func TestLoader(t *testing.T) {
	logger.Setup(logger.NewTestingLogWriter(t), GetLogLevel())
	s := stub.NewStubBigQuery()
	defer s.Close()

	loader := bqin.NewLoader(
		option.WithoutAuthentication(),
		option.WithEndpoint(s.Endpoint()),
	)

	cases := []struct {
		Comment    string
		ObjectURIs []string
		IsErr      bool
		*bqin.LoadingDestination
	}{
		{
			Comment:    "success default config",
			ObjectURIs: []string{"gs://my-bucket/my-object.csv"},
			LoadingDestination: &bqin.LoadingDestination{
				ProjectID: "my-project",
				Dataset:   "my-dataset",
				Table:     "my-table",
			},
			IsErr: false,
		},
		{
			Comment:    "missing project id",
			ObjectURIs: []string{"gs://my-bucket/my-object.csv"},
			LoadingDestination: &bqin.LoadingDestination{
				ProjectID: "",
				Dataset:   "my-dataset",
				Table:     "my-table",
			},
			IsErr: true,
		},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("case-%02d", i), func(t *testing.T) {
			t.Log(c.Comment)
			job := bqin.NewLoadingJob(c.LoadingDestination, c.ObjectURIs...)
			err := loader.Load(context.Background(), job)
			t.Logf("err is %v", err)
			if (err != nil) != c.IsErr {
				t.Error("unexpected error state")
			}
		})
	}
}
