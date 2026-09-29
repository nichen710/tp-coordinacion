package middleware

import (
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type queueMiddleware struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	queueName   string
	consumerTag string
}

func newQueueMiddleware(queueName string, connSettings ConnSettings) (Middleware, error) {
	if queueName == "" {
		return nil, errors.New(errorEmptyQueueName)
	}

	conn, ch, err := newConnectionAndChannel(connSettings)
	if err != nil {
		return nil, err
	}

	// Idempotent queue declaration: if exist do nothing
	_, err = ch.QueueDeclare(queueName, false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf(errorQueueDeclare, queueName, err)
	}

	consumerTag := fmt.Sprintf(consumerTagFormat, queueName, time.Now().UnixNano())

	return &queueMiddleware{
		conn:        conn,
		ch:          ch,
		queueName:   queueName,
		consumerTag: consumerTag,
	}, nil
}

func (q *queueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if err := checkConnection(q.conn); err != nil {
		return err
	}

	msgs, err := q.ch.Consume(q.queueName, q.consumerTag, false, false, false, false, nil)
	if err != nil {
		return handleErr(q.conn, err)
	}

	for msg := range msgs {
		ack := func() { msg.Ack(false) }
		nack := func() { msg.Nack(false, true) }
		callbackFunc(Message{Body: string(msg.Body)}, ack, nack)
	}

	return checkConnection(q.conn)
}

func (q *queueMiddleware) StopConsuming() error {
	if err := checkConnection(q.conn); err != nil {
		return err
	}

	err := q.ch.Cancel(q.consumerTag, false)
	return handleErr(q.conn, err)
}

func (q *queueMiddleware) Send(msg Message) error {
	if err := checkConnection(q.conn); err != nil {
		return err
	}

	err := q.ch.Publish("", q.queueName, false, false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	return handleErr(q.conn, err)
}

func (q *queueMiddleware) Close() error {
	return closeResources(q.ch, q.conn)
}
