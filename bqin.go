package bqin

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

var (
	ErrNoRequest = errors.New("no import request")
)

type ImportRequest struct {
	ID            string                 `json:"id"`
	ReceiptHandle string                 `json:"receipt_handle"`
	Records       []*ImportRequestRecord `json:"records"`
}

type ImportRequestRecord struct {
	SourceBucketName string `json:"source_bucket_name"`
	SourceObjectKey  string `json:"source_object_key"`
	TargetDataset    string `json:"target_dataset"`
	TargetTable      string `json:"target_table"`
}

type Receiver interface {
	Receive(context.Context) (*ImportRequest, error)
	Complete(context.Context, *ImportRequest) error
}

type Processor interface {
	Process(context.Context, *ImportRequest) error
}

type App struct {
	Receiver
	Processor

	mu sync.Mutex

	inShutdown atomicBool
	isAlive    atomicBool
	doneChan   chan struct{}
}

func NewApp(conf *Config) (*App, error) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config:            aws.Config{Region: aws.String(conf.AWS.Region)},
		SharedConfigState: session.SharedConfigEnable,
	}))
	receiver, err := NewSQSReceiver(sess, conf)
	if err != nil {
		errorf("receiver build error :%s", err)
		return nil, err
	}
	processor, err := NewBigQueryTransporter(conf, sess)
	if err != nil {
		errorf("processor build error :%s", err)
		return nil, err
	}
	return &App{
		Receiver:  receiver,
		Processor: processor,
	}, nil
}

func (app *App) ReceiveAndProcess() error {
	if app.Receiver == nil {
		return errors.New("Receiver is nil")
	}

	if app.Processor == nil {
		return errors.New("Processor is nil")
	}
	infof("Starting up bqin worker")
	defer infof("Shutdown bqin worker")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app.isAlive.set()
	defer app.isAlive.unset()

	for {
		select {
		case <-app.getDoneChan():
			return nil
		default:
		}

		req, err := app.Receive(ctx)
		switch err {
		case nil:
			// next step.
		case ErrNoRequest:
			continue
		default:
			return err
		}

		if err := app.Process(ctx, req); err != nil {
			return err
		}

		if err := app.Complete(ctx, req); err != nil {
			return err
		}
	}
}

var shutdownPollInterval = 500 * time.Millisecond

func (app *App) Shutdown(ctx context.Context) error {

	app.closeDoneChan()
	ticker := time.NewTicker(shutdownPollInterval)
	defer ticker.Stop()
	for {
		if !app.isAlive.isSet() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

//doneChan controll functions. as net/http Server
func (app *App) getDoneChan() <-chan struct{} {
	app.mu.Lock()
	defer app.mu.Unlock()
	return app.getDoneChanLocked()
}

func (app *App) getDoneChanLocked() chan struct{} {
	if app.doneChan == nil {
		app.doneChan = make(chan struct{})
	}
	return app.doneChan
}

func (app *App) closeDoneChanLocked() {
	ch := app.getDoneChanLocked()
	select {
	case <-ch:
		// Already closed. Don't close again.
	default:
		close(ch)
	}
}

func (app *App) closeDoneChan() {
	app.mu.Lock()
	defer app.mu.Unlock()
	app.closeDoneChanLocked()
}

type atomicBool int32

func (b *atomicBool) isSet() bool { return atomic.LoadInt32((*int32)(b)) != 0 }
func (b *atomicBool) set()        { atomic.StoreInt32((*int32)(b), 1) }
func (b *atomicBool) unset()      { atomic.StoreInt32((*int32)(b), 0) }
