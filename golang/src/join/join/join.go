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
	fruitItemMap      map[string]fruititem.FruitItem
	eofCount          int
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
		fruitItemMap:      map[string]fruititem.FruitItem{},
		eofCount:          0,
	}, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message in join", "err", err)
		return
	}

	if isEof {
		join.eofCount++
		if join.eofCount >= join.aggregationAmount {
			if err := join.sendFinalTop(); err != nil {
				slog.Error("While sending final top", "err", err)
			}
			join.fruitItemMap = map[string]fruititem.FruitItem{}
			join.eofCount = 0
		}
		return
	}

	for _, fruitRecord := range fruitRecords {
		if _, ok := join.fruitItemMap[fruitRecord.Fruit]; ok {
			join.fruitItemMap[fruitRecord.Fruit] = join.fruitItemMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			join.fruitItemMap[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (join *Join) sendFinalTop() error {
	fruitItems := make([]fruititem.FruitItem, 0, len(join.fruitItemMap))
	for _, item := range join.fruitItemMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(join.topSize, len(fruitItems))
	finalTop := fruitItems[:finalTopSize]

	message, err := inner.SerializeMessage(finalTop)
	if err != nil {
		return err
	}
	return join.outputQueue.Send(*message)
}
