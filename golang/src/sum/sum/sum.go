package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	id                   int
	sumAmount            int
	aggregationAmount    int
	inputQueue           middleware.Middleware
	aggregationExchanges []middleware.Middleware
	controlExchange      middleware.Middleware
	fruitItemMap         map[string]map[string]fruititem.FruitItem
	handledClients       map[string]bool
	mutex                sync.Mutex
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	aggregationExchanges := make([]middleware.Middleware, config.AggregationAmount)
	for i := range config.AggregationAmount {
		routingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, i)}
		ex, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, routingKey, connSettings)
		if err != nil {
			inputQueue.Close()
			for j := 0; j < i; j++ {
				aggregationExchanges[j].Close()
			}
			return nil, err
		}
		aggregationExchanges[i] = ex
	}

	var controlExchange middleware.Middleware
	if config.SumAmount > 1 {
		controlExchangeName := fmt.Sprintf("%s_control", config.SumPrefix)
		controlExchange, err = middleware.CreateExchangeMiddleware(controlExchangeName, []string{"eof"}, connSettings)
		if err != nil {
			inputQueue.Close()
			for _, ex := range aggregationExchanges {
				ex.Close()
			}
			return nil, err
		}
	}

	return &Sum{
		id:                   config.Id,
		sumAmount:            config.SumAmount,
		aggregationAmount:    config.AggregationAmount,
		inputQueue:           inputQueue,
		aggregationExchanges: aggregationExchanges,
		controlExchange:      controlExchange,
		fruitItemMap:         map[string]map[string]fruititem.FruitItem{},
		handledClients:       map[string]bool{},
	}, nil
}

func (sum *Sum) Run() {
	go sum.handleSignals()

	if sum.sumAmount > 1 {
		go sum.controlExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			defer ack()
			innerMsg, err := inner.DeserializeMessage(&msg)
			if err != nil {
				slog.Error("While deserializing control message", "err", err)
				return
			}
			if innerMsg.IsEof {
				sum.handleEndOfRecordMessage(innerMsg.ClientID)
			}
		})
	}

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	innerMsg, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if innerMsg.IsEof {
		if err := sum.handleEndOfRecordMessage(innerMsg.ClientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}

		if sum.sumAmount == 1 {
			return
		}

		if err := sum.controlExchange.Send(msg); err != nil {
			slog.Error("While sending EOF to control exchange", "err", err)
		}

		return
	}

	if err := sum.handleDataMessage(innerMsg.ClientID, innerMsg.Records); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientID string) error {
	sum.mutex.Lock()
	defer sum.mutex.Unlock()

	if sum.handledClients[clientID] {
		return nil
	}
	sum.handledClients[clientID] = true

	slog.Info("Received End Of Records message", "id", sum.id, "clientID", clientID)

	clientFruits, ok := sum.fruitItemMap[clientID]
	if ok {
		for key := range clientFruits {
			fruitRecord := clientFruits[key]
			message, err := inner.SerializeMessage(inner.InnerMessage{
				ClientID: clientID,
				Records:  []fruititem.FruitItem{fruitRecord},
				IsEof:    false,
			})
			if err != nil {
				slog.Debug("While serializing message", "err", err)
				return err
			}
			targetAggregator := int(hashFruit(fruitRecord.Fruit) % uint32(sum.aggregationAmount))
			if err := sum.aggregationExchanges[targetAggregator].Send(*message); err != nil {
				slog.Debug("While sending message", "err", err)
				return err
			}
		}
		delete(sum.fruitItemMap, clientID)
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
	for i := 0; i < sum.aggregationAmount; i++ {
		if err := sum.aggregationExchanges[i].Send(*eofMessage); err != nil {
			slog.Debug("While sending EOF message", "err", err)
			return err
		}
	}
	return nil
}

func (sum *Sum) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) error {
	sum.mutex.Lock()
	defer sum.mutex.Unlock()

	clientFruits, ok := sum.fruitItemMap[clientID]
	if !ok {
		clientFruits = make(map[string]fruititem.FruitItem)
		sum.fruitItemMap[clientID] = clientFruits
	}
	for _, fruitRecord := range fruitRecords {
		if existing, ok := clientFruits[fruitRecord.Fruit]; ok {
			clientFruits[fruitRecord.Fruit] = existing.Sum(fruitRecord)
		} else {
			clientFruits[fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}

func hashFruit(fruit string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(fruit))
	return h.Sum32()
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	sum.inputQueue.Close()
	if sum.controlExchange != nil {
		sum.controlExchange.Close()
	}
	for _, ex := range sum.aggregationExchanges {
		ex.Close()
	}
}
