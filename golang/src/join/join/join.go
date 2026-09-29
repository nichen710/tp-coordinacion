package join

import (
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	topSize           int
	aggregationAmount int
	fruitItemMap      map[string]map[string]fruititem.FruitItem
	eofCount          map[string]int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		topSize:           config.TopSize,
		aggregationAmount: config.AggregationAmount,
		fruitItemMap:      map[string]map[string]fruititem.FruitItem{},
		eofCount:          map[string]int{},
	}, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	innerMsg, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message in join", "err", err)
		return
	}

	if innerMsg.IsEof {
		join.eofCount[innerMsg.ClientID]++
		if join.eofCount[innerMsg.ClientID] >= join.aggregationAmount {
			if err := join.sendFinalTop(innerMsg.ClientID); err != nil {
				slog.Error("While sending final top", "err", err)
			}
			delete(join.fruitItemMap, innerMsg.ClientID)
			delete(join.eofCount, innerMsg.ClientID)
		}
		return
	}

	clientFruits, ok := join.fruitItemMap[innerMsg.ClientID]
	if !ok {
		clientFruits = make(map[string]fruititem.FruitItem)
		join.fruitItemMap[innerMsg.ClientID] = clientFruits
	}

	for _, fruitRecord := range innerMsg.Records {
		if _, ok := clientFruits[fruitRecord.Fruit]; ok {
			clientFruits[fruitRecord.Fruit] = clientFruits[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			clientFruits[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (join *Join) sendFinalTop(clientID string) error {
	clientFruits := join.fruitItemMap[clientID]
	fruitItems := make([]fruititem.FruitItem, 0, len(clientFruits))
	for _, item := range clientFruits {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(join.topSize, len(fruitItems))
	finalTop := fruitItems[:finalTopSize]

	message, err := inner.SerializeMessage(inner.InnerMessage{
		ClientID: clientID,
		Records:  finalTop,
		IsEof:    false,
	})
	if err != nil {
		return err
	}
	return join.outputQueue.Send(*message)
}
