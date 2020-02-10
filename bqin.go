package bqin

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kayac/bqin/cloud"
	"github.com/kayac/bqin/internal/logger"
)

var (
	ErrNoRequest = errors.New("no import request")
)

type ImportRequest struct {
	ID            string                 `json:"id,omitempty"`
	ReceiptHandle string                 `json:"receipt_handle,omitempty"`
	Records       []*ImportRequestRecord `json:"records"`
}

type ImportRequestRecord struct {
	Source *ImportSource `json:"source"`
	Target *ImportTarget `json:"target"`
}

type ImportSource struct {
	Bucket string `json:"bucket"`
	Object string `json:"object"`
}

func (s ImportSource) String() string {
	return fmt.Sprintf(S3URITemplate, s.Bucket, s.Object)
}

type ImportTarget struct {
	ProjectID string `json:"project_id"`
	Dataset   string `json:"dataset"`
	Table     string `json:"table"`
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
	c := cloud.New(conf.Cloud)
	receiver, err := NewSQSReceiver(conf, c)
	if err != nil {
		logger.Errorf("receiver build error :%s", err)
		return nil, err
	}
	return &App{
		Receiver:  receiver,
		Processor: NewBigQueryTransporter(conf, c),
	}, nil
}

func (app *App) ReceiveAndProcess() error {
	logger.Infof("Starting up bqin worker")
	defer logger.Infof("Shutdown bqin worker")

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
		if err := app.OneReceiveAndProcess(ctx); err != nil && err != ErrNoRequest {
			return err
		}
	}
}

func (app *App) OneReceiveAndProcess(ctx context.Context) error {
	if app.Receiver == nil {
		return errors.New("Receiver is nil")
	}

	if app.Processor == nil {
		return errors.New("Processor is nil")
	}
	req, err := app.Receive(ctx)
	if err != nil {
		return err
	}
	if err := app.Process(ctx, req); err != nil {
		return err
	}

	if err := app.Complete(ctx, req); err != nil {
		return err
	}
	return nil
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
