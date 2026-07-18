package main

import (
	"kafka-lite/internal/broker"
	"kafka-lite/internal/logger"
	"path/filepath"
)

const (
	address        = ":9092"
	dataDirectory  = "data"
	maxSegmentSize = 10 << 20 // 10 MiB
)

type TopicConfig struct {
	Name       string
	Partitions int
}

var topics = []TopicConfig{
	{
		Name:       "orders",
		Partitions: 3,
	},
	{
		Name:       "payments",
		Partitions: 2,
	},
	{
		Name:       "logs",
		Partitions: 1,
	},
}

func main() {
	if err := logger.Init("broker", logger.LevelInfo); err != nil {
		panic(err)
	}
	defer logger.Close()

	broker, err := broker.NewBroker(address)
	if err != nil {
		logger.Fatal(
			"broker_create_failed",
			logger.Err(err),
		)
	}

	defer broker.Close()

	for _, topic := range topics {
		if err := broker.CreateTopic(topic.Name, filepath.Join(dataDirectory, topic.Name), topic.Partitions, maxSegmentSize); err != nil {
			logger.Fatal(
				"topic_create_failed",
				logger.Str("topic", topic.Name),
				logger.Err(err),
			)
		}
	}

	logger.Info(
		"broker_started",
		logger.Str("address", address),
	)

	if err = broker.Start(); err != nil {
		logger.Fatal(
			"broker_start_failed",
			logger.Err(err),
		)
	}
}
