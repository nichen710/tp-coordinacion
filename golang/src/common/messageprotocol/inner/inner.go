package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type InnerMessage struct {
	ClientID string
	Records  []fruititem.FruitItem
	IsEof    bool
}

func serializeJson(message []interface{}) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJson(message []byte) ([]interface{}, error) {
	var data []interface{}
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return data, nil
}

func SerializeMessage(innerMessage InnerMessage) (*middleware.Message, error) {
	recordsData := []interface{}{}
	if !innerMessage.IsEof {
		for _, fruitRecord := range innerMessage.Records {
			datum := []interface{}{
				fruitRecord.Fruit,
				fruitRecord.Amount,
			}
			recordsData = append(recordsData, datum)
		}
	}

	data := []interface{}{
		innerMessage.ClientID,
		recordsData,
	}

	body, err := serializeJson(data)
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (*InnerMessage, error) {
	data, err := deserializeJson([]byte((*message).Body))
	if err != nil {
		return nil, err
	}

	if len(data) != 2 {
		return nil, errors.New("message must contain [clientId, records]")
	}

	clientID, ok := data[0].(string)
	if !ok {
		return nil, errors.New("clientId must be a string")
	}

	recordsData, ok := data[1].([]interface{})
	if !ok {
		return nil, errors.New("records must be an array")
	}

	fruitRecords := []fruititem.FruitItem{}
	for _, datum := range recordsData {
		fruitPair, ok := datum.([]interface{})
		if !ok {
			return nil, errors.New("Datum is not an array")
		}

		fruit, ok := fruitPair[0].(string)
		if !ok {
			return nil, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitAmount, ok := fruitPair[1].(float64)
		if !ok {
			return nil, errors.New("Datum is not a (fruit, amount) pair")
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(fruitAmount)}
		fruitRecords = append(fruitRecords, fruitRecord)
	}

	return &InnerMessage{
		ClientID: clientID,
		Records:  fruitRecords,
		IsEof:    len(fruitRecords) == 0,
	}, nil
}
