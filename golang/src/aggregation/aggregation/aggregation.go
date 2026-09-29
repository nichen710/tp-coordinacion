package aggregation

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	fruitItemMap  map[string]map[string]fruititem.FruitItem
	eofCount      map[string]int
	sumAmount     int
	topSize       int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		fruitItemMap:  map[string]map[string]fruititem.FruitItem{},
		eofCount:      map[string]int{},
		sumAmount:     config.SumAmount,
		topSize:       config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	go aggregation.handleSignals()

	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	innerMsg, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if innerMsg.IsEof {
		aggregation.eofCount[innerMsg.ClientID]++
		if aggregation.eofCount[innerMsg.ClientID] >= aggregation.sumAmount {
			if err := aggregation.handleEndOfRecordsMessage(innerMsg.ClientID); err != nil {
				slog.Error("While handling end of record message", "err", err)
			}
			delete(aggregation.fruitItemMap, innerMsg.ClientID)
			delete(aggregation.eofCount, innerMsg.ClientID)
		}
		return
	}

	aggregation.handleDataMessage(innerMsg.ClientID, innerMsg.Records)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID string) error {
	slog.Info("Received all End Of Records messages for client", "clientID", clientID)

	clientFruits := aggregation.fruitItemMap[clientID]
	fruitTopRecords := aggregation.buildFruitTop(clientFruits)
	message, err := inner.SerializeMessage(inner.InnerMessage{
		ClientID: clientID,
		Records:  fruitTopRecords,
		IsEof:    false,
	})
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	eofMessage, err := inner.SerializeMessage(inner.InnerMessage{
		ClientID: clientID,
		Records:  []fruititem.FruitItem{},
		IsEof:    true,
	})
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*eofMessage); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) {
	clientFruits, ok := aggregation.fruitItemMap[clientID]
	if !ok {
		clientFruits = make(map[string]fruititem.FruitItem)
		aggregation.fruitItemMap[clientID] = clientFruits
	}
	for _, fruitRecord := range fruitRecords {
		if _, ok := clientFruits[fruitRecord.Fruit]; ok {
			clientFruits[fruitRecord.Fruit] = clientFruits[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			clientFruits[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientFruits map[string]fruititem.FruitItem) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(clientFruits))
	for _, item := range clientFruits {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}

func (aggregation *Aggregation) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	aggregation.inputExchange.Close()
	aggregation.outputQueue.Close()
}
