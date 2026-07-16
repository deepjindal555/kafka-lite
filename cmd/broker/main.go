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

var topics = []string{
	"orders",
	"payments",
	"logs",
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
		if err := broker.CreateTopic(topic, filepath.Join(dataDirectory, topic), maxSegmentSize); err != nil {
			logger.Fatal(
				"topic_create_failed",
				logger.Str("topic", topic),
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
