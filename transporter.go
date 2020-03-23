package bqin

import (
	"context"
	"fmt"
	"io"
	"net/url"

	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/kayac/bqin/internal/logger"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
)

type Transporter struct {

	//for s3 client session
	sess *session.Session

	//for gcp cloud strage client options
	opts []option.ClientOption
}

func NewTransporter(sess *session.Session, opts ...option.ClientOption) *Transporter {
	return &Transporter{
		sess: sess,
		opts: opts,
	}
}

type TransportJob struct {
	Source      *url.URL
	Destination *url.URL
}

func (job *TransportJob) String() string {
	return fmt.Sprintf("transport from %s to %s", job.Source, job.Destination)
}

type TransportJobHandle struct {
	locator *url.URL
	obj     *storage.ObjectHandle
}

func (t *Transporter) Transport(ctx context.Context, job *TransportJob) (*TransportJobHandle, error) {
	logger.Debugf("try %s", job)
	reader, err := t.newReader(ctx, job.Source)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	writer, obj, err := t.newWriter(ctx, job.Destination)
	if err != nil {
		return nil, err
	}
	defer writer.Close()

	_, err = io.Copy(writer, reader)
	if err != nil {
		return nil, errors.Wrap(err, "copy object failed")
	}
	logger.Debugf("toransport job successed")
	handle := &TransportJobHandle{
		locator: job.Destination,
		obj:     obj,
	}
	return handle, nil
}

func (t *Transporter) newReader(ctx context.Context, loc *url.URL) (io.ReadCloser, error) {
	if loc.Scheme != "s3" {
		return nil, errors.New("source is not s3 object")
	}

	resp, err := s3.New(t.sess).GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(loc.Host),
		Key:    aws.String(loc.Path),
	})
	if err != nil {
		return nil, errors.Wrap(err, "get object from s3 failed")
	}
	logger.Debugf("get object from %s successed.", loc)
	return resp.Body, nil
}

func (t *Transporter) newWriter(ctx context.Context, loc *url.URL) (io.WriteCloser, *storage.ObjectHandle, error) {
	if loc.Scheme != "gs" {
		return nil, nil, errors.New("destination is not google cloud storage object")
	}
	gcs, err := storage.NewClient(ctx, t.opts...)
	if err != nil {
		return nil, nil, errors.Wrap(err, "can not get cloud storage client")
	}
	obj := gcs.Bucket(loc.Host).Object(loc.Path)
	return obj.NewWriter(ctx), obj, nil
}

var ErrInvalidHandle = errors.New("invalid handle")

func (h *TransportJobHandle) Cleanup(ctx context.Context) error {
	if h == nil {
		logger.Errorf("try cleanup but job handle is nil")
		return ErrInvalidHandle
	}
	if h.obj == nil {
		logger.Errorf("try cleanup but object handle is nil")
		return ErrInvalidHandle
	}
	logger.Debugf("cleanup %s", h.locator)
	if err := h.obj.Delete(ctx); err != nil {
		if err != storage.ErrObjectNotExist {
			logger.Errorf("can not delete temporary object reason: %s", err)
			return err
		}
		logger.Debugf("aleady cleanuped %s", h.locator)
	}
	return nil
}
