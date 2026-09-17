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
	queue       *amqp.Queue
	exchange    string
	topics      []string
	isConsuming bool
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

	if err := channel.Qos(1, 0, false); err != nil {
		_ = conn.Close()
		_ = channel.Close()
		return nil, err
	}

	err = exchangeDeclare(exchange, channel)
	if err != nil {
		_ = conn.Close()
		_ = channel.Close()
		return nil, err
	}

	queue, err := anonymousQueueDeclare(channel)
	if err != nil {
		_ = conn.Close()
		_ = channel.Close()
		return nil, err
	}

	err = bindQueueToTopics(&queue, channel, exchange, keys)
	if err != nil {
		_ = conn.Close()
		_ = channel.Close()
		return nil, err
	}

	return &ExchangeMiddleware{
		conn:     conn,
		channel:  channel,
		queue:    &queue,
		exchange: exchange,
		topics:   keys,
	}, nil
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	msgs, err := em.consume()
	if err != nil {
		return err
	}

	defer func() { em.isConsuming = false }()
	em.isConsuming = true

	for d := range msgs {
		msg := m.Message{Body: string(d.Body)}
		ack := func() { _ = d.Ack(false) }
		nack := func() { _ = d.Nack(false, true) }

		callbackFunc(msg, ack, nack)
	}

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
		err := em.publish(msg, topic)
		if err != nil {
			return err
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

func bindQueueToTopics(queue *amqp.Queue, channel *amqp.Channel, exchange string, topics []string) error {
	for _, topic := range topics {
		if channel.QueueBind(
			queue.Name,
			topic,
			exchange,
			false,
			nil,
		) != nil {
			return m.ErrMessageMiddlewareMessage
		}
	}
	return nil
}

func anonymousQueueDeclare(channel *amqp.Channel) (amqp.Queue, error) {
	queue, err := channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		return amqp.Queue{}, m.ErrMessageMiddlewareMessage
	}

	return queue, nil
}

func (em *ExchangeMiddleware) publish(msg m.Message, topic string) error {
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
	return nil
}

func (em *ExchangeMiddleware) consume() (<-chan amqp.Delivery, error) {
	msgs, err := em.channel.Consume(
		em.queue.Name,
		em.consumerTag(),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		if em.isDisconnectedErr(err) {
			return nil, m.ErrMessageMiddlewareDisconnected
		}
		return nil, m.ErrMessageMiddlewareMessage
	}
	return msgs, nil
}

func exchangeDeclare(exchange string, channel *amqp.Channel) error {
	return channel.ExchangeDeclare(
		exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
}
