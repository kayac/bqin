package bqin

import (
	"context"
	"errors"

	"github.com/kayac/bqin/internal/logger"
)

type App struct {
	*Receiver
	*Resolver
	*Transporter
	*Loader
}

func NewApp(conf *Config) *App {
	factory := &Factory{Config: conf}
	return factory.NewApp()
}

func (app *App) Run(ctx context.Context, opts ...RunOption) error {
	logger.Infof("Starting up bqin worker")
	defer logger.Infof("Shutdown bqin worker")
	settings := &RunSettings{}
	for _, opt := range opts {
		opt.Apply(settings)
	}
	if settings.QueueName != "" {
		defaultQueueName := app.GetQueueName()
		app.SetQueueName(settings.QueueName)
		defer app.SetQueueName(defaultQueueName)
	}

	for {
		select {
		case <-ctx.Done():
			if err := ctx.Err(); err != context.Canceled {
				return err
			}
			logger.Infof("canceled")
			return nil
		default:
		}

		switch err := app.batch(context.Background()); err {
		case ErrNoMessage:
			if settings.ExitNoMessage {
				logger.Infof("success all")
				return nil
			}
		case nil:
			//nothing todo
		default:
			if settings.ExitError {
				return err
			}
			logger.Errorf("process failed. reason:%s", err)
		}
	}
}

func (app *App) batch(ctx context.Context) error {
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

	for i, job := range jobs {
		receiptHandle.Infof("[job %02d]%s", i, job)
		transportHandle, err := app.Transport(ctx, job.TransportJob)
		if err != nil {
			return err
		}
		transportHandles = append(transportHandles, transportHandle)
		if err := app.Load(ctx, job.LoadingJob); err != nil {
			return err
		}
		receiptHandle.Infof("[job %02d]complte job", i)
	}
	return receiptHandle.Complete()
}

type RunOption interface {
	Apply(*RunSettings)
}

type RunSettings struct {
	ExitNoMessage bool
	ExitError     bool
	QueueName     string
}

func (s *RunSettings) Apply(o *RunSettings) {
	o.ExitNoMessage = s.ExitNoMessage
	o.ExitError = s.ExitError
}

type withExitNoMessage bool

func (opt withExitNoMessage) Apply(settings *RunSettings) {
	settings.ExitNoMessage = bool(opt)
}

func WithExitNoMessage(flag bool) RunOption {
	return withExitNoMessage(flag)
}

type withExitError bool

func (opt withExitError) Apply(settings *RunSettings) {
	settings.ExitError = bool(opt)
}

func WithExitError(flag bool) RunOption {
	return withExitError(flag)
}

type withQueueName string

func (opt withQueueName) Apply(settings *RunSettings) {
	settings.QueueName = string(opt)
}

func WithQueueName(queueName string) RunOption {
	return withQueueName(queueName)
}
