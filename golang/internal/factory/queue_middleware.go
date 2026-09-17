package factory

import (
	"errors"
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn        *amqp.Connection
	channel     *amqp.Channel
	queue       amqp.Queue
	isConsuming bool
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	msgs, err := qm.channel.Consume(
		qm.queue.Name,
		qm.queue.Name,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		if qm.isDisconnectedErr(err) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	qm.isConsuming = true
	for d := range msgs {
		msg := m.Message{Body: string(d.Body)}
		ack := func() { _ = d.Ack(false) }
		nack := func() { _ = d.Nack(false, true) }

		callbackFunc(msg, ack, nack)
	}
	qm.isConsuming = false

	if qm.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	if !qm.isConsuming {
		return nil
	}

	if err := qm.channel.Cancel(qm.queue.Name, false); err != nil {
		if qm.isDisconnectedErr(err) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	return nil
}

func (qm *QueueMiddleware) Send(msg m.Message) error {
	err := qm.channel.Publish(
		"",
		qm.queue.Name,
		false,
		false,
		amqp.Publishing{
			Body: []byte(msg.Body),
		},
	)

	if err != nil {
		if qm.isDisconnectedErr(err) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	return nil
}

func (qm *QueueMiddleware) Close() error {
	var closeErr error

	if err := qm.channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		closeErr = err
	}
	if err := qm.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		closeErr = err
	}

	if closeErr != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}

func (qm *QueueMiddleware) isDisconnectedErr(err error) bool {
	return qm.conn.IsClosed() || errors.Is(err, amqp.ErrClosed)
}

func NewQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%d", connectionSettings.Hostname, connectionSettings.Port))
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	queue, err := channel.QueueDeclare(
		queueName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return nil, err
	}

	return &QueueMiddleware{
		conn:    conn,
		channel: channel,
		queue:   queue,
	}, nil
}
