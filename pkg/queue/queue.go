package queue

import (
	"context"
	"time"
)

type Queue interface {
	Publish(ctx context.Context, subject string, msg []byte) error
	Subscribe(ctx context.Context, subject string, handler MessageHandler, opts ...SubscribeOpt) error
	Close() error
	Purge(ctx context.Context) error
}

func NewQueue(opts ...Option) (Queue, error) {
	defaultOpts := defaultOptions()

	for _, opt := range opts {
		opt(defaultOpts)
	}

	return newNatsQueue(defaultOpts)
}

// 队列配置
type options struct {
	Url    string        // 服务连接地址
	Name   string        // 队列名称
	MaxAge time.Duration // 消息最大存活时间
}

type Option func(*options)

func WithUrl(url string) Option {
	return func(o *options) {
		o.Url = url
	}
}

func WithName(name string) Option {
	return func(o *options) {
		o.Name = name
	}
}

func WithMaxAge(maxAge time.Duration) Option {
	return func(o *options) {
		o.MaxAge = maxAge
	}
}

// 默认队列配置
func defaultOptions() *options {
	return &options{
		Url:    "nats://127.0.0.1:4222",
		Name:   "default-queue",
		MaxAge: 7 * 24 * time.Hour,
	}
}

// 消息处理函数
type MessageHandler func(msg []byte) error

// 消费者订阅选项
type subscribeOpts struct {
	Durable bool // 是否持久化
}

type SubscribeOpt func(o *subscribeOpts)

func WithDurable(durable bool) SubscribeOpt {
	return func(o *subscribeOpts) {
		o.Durable = durable
	}
}

func defaultSubscribeOpts() *subscribeOpts {
	return &subscribeOpts{
		Durable: true,
	}
}
