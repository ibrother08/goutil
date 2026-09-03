package queue

import (
	"context"
	"time"
)

type Queue interface {
	Publish(ctx context.Context, subject string, msg []byte) error
	Subscribe(ctx context.Context, subject string, handler MessageHandler) error
	Close() error
}

type MessageHandler func(msg []byte) error

type QueueConfig struct {
	Url    string
	Name   string
	MaxAge time.Duration
}

var DefaultQueueConfig = &QueueConfig{
	Name:   "default",
	MaxAge: 3 * 24 * time.Hour,
}

func NewQueue(cfg *QueueConfig) (Queue, error) {
	return NewNatsQueue(&NatsConfig{
		Url: cfg.Url,
	})
}
