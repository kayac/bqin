package bqin

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kayac/bqin/internal/logger"
)

type App struct {
	*Receiver
	*Resolver
	*Transporter
	*Loader

	mu         sync.Mutex
	inShutdown atomicBool
	isAlive    atomicBool
	doneChan   chan struct{}
}

func NewApp(conf *Config) *App {
	factory := &Factory{Config: conf}
	return factory.NewApp()
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
		if err := app.OneReceiveAndProcess(ctx); err != nil && err != ErrNoMessage {
			logger.Errorf("OneReceiveAndProcess failed. reason:%s", err)
		}
	}
}

func (app *App) OneReceiveAndProcess(ctx context.Context) error {
	urls, receiptHandle, err := app.Receive(ctx)
	defer receiptHandle.Cleanup()
	if err != nil {
		return err
	}
	jobs := app.Resolve(urls)
	if len(jobs) == 0 {
		return errors.New("nothing to do")
	}

	transportHandles := make([]*TransportJobHandle, 0, len(jobs))
	defer func() {
		for _, h := range transportHandles {
			h.Cleanup(ctx)
		}
	}()

	for _, job := range jobs {
		transportHandle, err := app.Transport(ctx, job.TransportJob)
		if err != nil {
			return err
		}
		transportHandles = append(transportHandles, transportHandle)
		if err := app.Load(ctx, job.LoadingJob); err != nil {
			return err
		}
	}
	receiptHandle.Complete()
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
