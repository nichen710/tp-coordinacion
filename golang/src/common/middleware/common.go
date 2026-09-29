package middleware

import (
	"errors"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// Commons Constants
	rabbitmqBaseURL   = "amqp://guest:guest@%s:%d/"
	consumerTagFormat = "%s-%d"

	// Commons Errors
	errorInvalidConnectionSettings = "error invalid connection settings, hostname: '%s', port: %d"
	errorConnectionFailed          = "error connecting to rabbitmq: %w"
	errorChannelOpen               = "error opening channel: %w"

	// Queue Errors
	errorEmptyQueueName = "queue name cannot be empty"
	errorQueueDeclare   = "error declaring queue %s: %w"

	// Exchange Errors
	errorEmptyExchangeName = "exchange name cannot be empty"
	errorExchangeDeclare   = "error declaring exchange %s: %w"
)

// newConnectionAndChannel creates a new connection and channel to RabbitMQ broker
func newConnectionAndChannel(connSettings ConnSettings) (*amqp.Connection, *amqp.Channel, error) {
	if connSettings.Hostname == "" || connSettings.Port <= 0 {
		return nil, nil, fmt.Errorf(errorInvalidConnectionSettings, connSettings.Hostname, connSettings.Port)
	}

	url := fmt.Sprintf(rabbitmqBaseURL, connSettings.Hostname, connSettings.Port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, fmt.Errorf(errorConnectionFailed, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf(errorChannelOpen, err)
	}

	return conn, ch, nil
}

// checkConnection verifies if the connection is alive
// If not return ErrMessageMiddlewareDisconnected
func checkConnection(conn *amqp.Connection) error {
	if conn == nil || conn.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

// handleErr handles errors from RabbitMQ
// If the connection is closed, return ErrMessageMiddlewareDisconnected
// Otherwise return ErrMessageMiddlewareMessage
func handleErr(conn *amqp.Connection, err error) error {
	if err == nil {
		return nil
	}
	if conn == nil || conn.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	return ErrMessageMiddlewareMessage
}

// closeResources closes the channel and connection
// It returns ErrMessageMiddlewareClose if the close operation fails
func closeResources(ch *amqp.Channel, conn *amqp.Connection) error {
	var closeErr error

	if ch != nil {
		if err := ch.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			closeErr = ErrMessageMiddlewareClose
		}
	}

	if conn != nil {
		if err := conn.Close(); err != nil && !errors.Is(err, amqp.ErrClosed) {
			closeErr = ErrMessageMiddlewareClose
		}
	}

	return closeErr
}
