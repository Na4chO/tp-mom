package factory

import (
	"errors"
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	conn        *amqp.Connection
	channel     *amqp.Channel
	exchange    string
	topics      []string
	isConsuming bool
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	queue, err := em.channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	for _, topic := range em.topics {
		err = em.channel.QueueBind(
			queue.Name,
			topic,
			em.exchange,
			false,
			nil,
		)
		if err != nil {
			return m.ErrMessageMiddlewareMessage
		}
	}

	msgs, err := em.channel.Consume(
		queue.Name,
		em.consumerTag(),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		if em.isDisconnectedErr(err) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	em.isConsuming = true
	for d := range msgs {
		msg := m.Message{Body: string(d.Body)}
		ack := func() { _ = d.Ack(false) }
		nack := func() { _ = d.Nack(false, true) }

		callbackFunc(msg, ack, nack)
	}
	em.isConsuming = false

	if em.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	return nil
}

func (em *ExchangeMiddleware) StopConsuming() error {
	if !em.isConsuming {
		return nil
	}

	if err := em.channel.Cancel(em.consumerTag(), false); err != nil {
		if em.isDisconnectedErr(err) {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	return nil
}

func (em *ExchangeMiddleware) Send(msg m.Message) error {
	for _, topic := range em.topics {
		err := em.channel.Publish(
			em.exchange,
			topic,
			false,
			false,
			amqp.Publishing{
				Body: []byte(msg.Body),
			},
		)
		if err != nil {
			if em.isDisconnectedErr(err) {
				return m.ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}
	}

	return nil
}

func (em *ExchangeMiddleware) Close() error {
	var closeErr error

	if err := em.channel.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		closeErr = err
	}
	if err := em.conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
		closeErr = err
	}

	if closeErr != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}

func (em *ExchangeMiddleware) isDisconnectedErr(err error) bool {
	return em.conn.IsClosed() || errors.Is(err, amqp.ErrClosed)
}

func (em *ExchangeMiddleware) consumerTag() string {
	return em.exchange
}

func NewExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%d", connectionSettings.Hostname, connectionSettings.Port))
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}

	err = channel.ExchangeDeclare(
		exchange,
		"topic",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = conn.Close()
		_ = channel.Close()
		return nil, err
	}

	return &ExchangeMiddleware{
		conn:     conn,
		channel:  channel,
		exchange: exchange,
		topics:   keys,
	}, nil
}
