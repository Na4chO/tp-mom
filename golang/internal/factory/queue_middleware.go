package factory

import (
	"errors"
	"sync/atomic"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn        *amqp.Connection
	channel     *amqp.Channel
	queue       amqp.Queue
	consumerTag string
	isConsuming atomic.Bool
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	if qm.isConsuming.Load() {
		return nil
	}

	msgs, err := qm.channel.Consume(
		qm.queue.Name,
		qm.consumerTag,
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

	qm.isConsuming.Store(true)

	go func() {
		defer qm.isConsuming.Store(false)

		for d := range msgs {
			msg := m.Message{Body: string(d.Body)}
			ack := func() { _ = d.Ack(false) }
			nack := func() { _ = d.Nack(false, true) }

			callbackFunc(msg, ack, nack)
		}
	}()

	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	if !qm.isConsuming.Load() {
		return nil
	}

	if qm.channel.Cancel(qm.consumerTag, false) != nil {
		return m.ErrMessageMiddlewareDisconnected
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
			// TODO: Decidir si hacerlo persistent o no
			//DeliveryMode: amqp.Persistent,
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
	// LLAMO A STOP CONSUMING ???

	if qm.channel.Close() != nil {
		return m.ErrMessageMiddlewareClose
	}

	if qm.conn.Close() != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}

func (qm *QueueMiddleware) isDisconnectedErr(err error) bool {
	return qm.conn.IsClosed() || errors.Is(err, amqp.ErrClosed)
}
