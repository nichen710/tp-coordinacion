package messagehandler

import (
	"fmt"
	"sync/atomic"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var nextClientID uint64

type MessageHandler struct {
	clientID string
}

func NewMessageHandler() MessageHandler {
	id := atomic.AddUint64(&nextClientID, 1)
	return MessageHandler{
		clientID: fmt.Sprintf("client-%d", id),
	}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	innerMsg := inner.InnerMessage{
		ClientID: messageHandler.clientID,
		Records:  []fruititem.FruitItem{fruitRecord},
		IsEof:    false,
	}
	return inner.SerializeMessage(innerMsg)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	innerMsg := inner.InnerMessage{
		ClientID: messageHandler.clientID,
		Records:  []fruititem.FruitItem{},
		IsEof:    true,
	}
	return inner.SerializeMessage(innerMsg)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	innerMsg, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}

	if innerMsg.ClientID != messageHandler.clientID {
		return nil, nil
	}

	return innerMsg.Records, nil
}
