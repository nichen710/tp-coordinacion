package middleware

import (
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type exchangeMiddleware struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	exchange    string
	keys        []string
	consumerTag string
}

func newExchangeMiddleware(exchange string, keys []string, connSettings ConnSettings) (Middleware, error) {
	if exchange == "" {
		return nil, errors.New(errorEmptyExchangeName)
	}

	conn, ch, err := newConnectionAndChannel(connSettings)
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(exchange, "direct", false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf(errorExchangeDeclare, exchange, err)
	}

	consumerTag := fmt.Sprintf(consumerTagFormat, exchange, time.Now().UnixNano())

	return &exchangeMiddleware{
		conn:        conn,
		ch:          ch,
		exchange:    exchange,
		keys:        keys,
		consumerTag: consumerTag,
	}, nil
}

func (e *exchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if err := checkConnection(e.conn); err != nil {
		return err
	}

	q, err := e.ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return handleErr(e.conn, err)
	}

	for _, key := range e.keys {
		err = e.ch.QueueBind(q.Name, key, e.exchange, false, nil)
		if err != nil {
			return handleErr(e.conn, err)
		}
	}

	msgs, err := e.ch.Consume(q.Name, e.consumerTag, false, false, false, false, nil)
	if err != nil {
		return handleErr(e.conn, err)
	}

	for msg := range msgs {
		ack := func() { msg.Ack(false) }
		nack := func() { msg.Nack(false, true) }
		callbackFunc(Message{Body: string(msg.Body)}, ack, nack)
	}

	return checkConnection(e.conn)
}

func (e *exchangeMiddleware) StopConsuming() error {
	if err := checkConnection(e.conn); err != nil {
		return err
	}

	err := e.ch.Cancel(e.consumerTag, false)
	return handleErr(e.conn, err)
}

func (e *exchangeMiddleware) Send(msg Message) error {
	if err := checkConnection(e.conn); err != nil {
		return err
	}

	for _, key := range e.keys {
		err := e.ch.Publish(e.exchange, key, false, false,
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(msg.Body),
			})
		if err != nil {
			return handleErr(e.conn, err)
		}
	}

	return nil
}

func (e *exchangeMiddleware) Close() error {
	return closeResources(e.ch, e.conn)
}
