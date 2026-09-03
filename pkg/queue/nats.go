package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type NatsQueue struct {
	conn       *nats.Conn
	jetStream  jetstream.JetStream
	stream     jetstream.Stream
	consumeCtx jetstream.ConsumeContext
}

type NatsConfig struct {
	Url    string
	Name   string
	MaxAge time.Duration
}

func NewNatsQueue(cfg *NatsConfig) (*NatsQueue, error) {
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

	return &NatsQueue{
		conn:      conn,
		jetStream: jetStream,
		stream:    stream,
	}, nil
}

func (q *NatsQueue) Publish(ctx context.Context, subject string, msg []byte) error {
	name := q.stream.CachedInfo().Config.Name

	_, err := q.jetStream.Publish(ctx, fmt.Sprintf("%s.%s", name, subject), msg)
	return err
}

func (q *NatsQueue) Subscribe(ctx context.Context, subject string, handler MessageHandler) error {
	name := q.stream.CachedInfo().Config.Name

	consumer, err := q.stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       fmt.Sprintf("%s-%s", name, subject),
		FilterSubject: fmt.Sprintf("%s.%s", name, subject),
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		return err
	}

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		if err := handler(msg.Data()); err != nil {
			msg.Nak()
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

func (q *NatsQueue) Close() error {
	q.consumeCtx.Stop()

	if err := q.conn.Drain(); err != nil {
		return err
	}

	return nil
}
