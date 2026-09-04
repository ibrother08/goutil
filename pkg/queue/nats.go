package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type natsQueue struct {
	cfg        *options
	conn       *nats.Conn
	jetStream  jetstream.JetStream
	stream     jetstream.Stream
	consumeCtx jetstream.ConsumeContext
}

func newNatsQueue(cfg *options) (*natsQueue, error) {
	conn, err := nats.Connect(cfg.Url)
	if err != nil {
		return nil, err
	}

	jetStream, err := jetstream.New(conn)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := jetStream.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      cfg.Name,
		Subjects:  []string{fmt.Sprintf("%s.>", cfg.Name)},
		MaxAge:    cfg.MaxAge,
		Retention: jetstream.LimitsPolicy,
	})
	if err != nil {
		return nil, err
	}

	return &natsQueue{
		cfg:       cfg,
		conn:      conn,
		jetStream: jetStream,
		stream:    stream,
	}, nil
}

func (q *natsQueue) Publish(ctx context.Context, subject string, msg []byte) error {
	name := q.cfg.Name

	_, err := q.jetStream.Publish(ctx, name+"."+subject, msg)

	return err
}

func (q *natsQueue) Subscribe(ctx context.Context, subject string, handler MessageHandler, opts ...SubscribeOpt) error {
	defaultOpts := defaultSubscribeOpts()

	for _, opt := range opts {
		opt(defaultOpts)
	}

	name := q.cfg.Name

	var durable, filterSubject string

	if subject == "" {
		durable = name
		filterSubject = name + ".>"
	} else {
		durable = name + "-" + subject
		filterSubject = name + "." + subject
	}

	if !defaultOpts.Durable {
		durable = ""
	}

	consumer, err := q.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       durable,
		FilterSubject: filterSubject,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return err
	}

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		if err := handler(msg.Data()); err != nil {
			return
		}

		msg.Ack()
	})
	if err != nil {
		return err
	}
	q.consumeCtx = consumeCtx

	return nil
}

func (q *natsQueue) Close() error {
	if q.consumeCtx != nil {
		q.consumeCtx.Stop()
	}

	if err := q.conn.Drain(); err != nil {
		return err
	}

	return nil
}

func (q *natsQueue) Purge(ctx context.Context) error {
	return q.stream.Purge(ctx)
}
