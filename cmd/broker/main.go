package main

import (
	"kafka-lite/internal/broker"
	"kafka-lite/internal/logger"
)

const (
	address        = ":9092"
	dataDirectory  = "data/default"
	maxSegmentSize = 10 << 20 // 10 MiB
)

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

	if err = broker.CreateTopic("default", dataDirectory, maxSegmentSize); err != nil {
		logger.Fatal(
			"topic_create_failed",
			logger.Str("topic", "default"),
			logger.Err(err),
		)
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
