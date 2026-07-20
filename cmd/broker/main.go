package main

import (
	"flag"
	"fmt"
	"kafka-lite/internal/broker"
	"kafka-lite/internal/logger"
	"os"
	"path/filepath"
)

const (
	defaultAddress    = ":9092"
	defaultTopicsFile = "topics.yaml"

	defaultDataDirectory = "data"
	defaultLogDirectory  = ""

	defaultMaxSegmentSize = 10 << 20 // 10 MiB
)

type BrokerConfig struct {
	Address    string
	TopicsFile string

	DataDirectory string
	LogDirectory  string

	MaxSegmentSize int64
}

func main() {
	config := BrokerConfig{
		Address:        defaultAddress,
		TopicsFile:     defaultTopicsFile,
		DataDirectory:  defaultDataDirectory,
		LogDirectory:   defaultLogDirectory,
		MaxSegmentSize: defaultMaxSegmentSize,
	}

	flag.StringVar(&config.Address, "address", defaultAddress, "broker listen address")
	flag.StringVar(&config.TopicsFile, "topics-file", defaultTopicsFile, "path to the broker topics configuration file")
	flag.StringVar(&config.DataDirectory, "data-directory", defaultDataDirectory, "broker data directory")
	flag.StringVar(&config.LogDirectory, "log-directory", defaultLogDirectory, "directory under logs/ where log files will be written")
	flag.Int64Var(&config.MaxSegmentSize, "max-segment-size", defaultMaxSegmentSize, "maximum segment size in bytes")

	flag.Parse()

	if config.MaxSegmentSize <= 0 {
		fmt.Fprintln(os.Stderr, "broker: --max-segment-size must be greater than 0")
		os.Exit(2)
	}

	topics, err := loadTopics(config.TopicsFile)
	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"broker: failed to load topics from %q: %v\n",
			config.TopicsFile,
			err,
		)
		os.Exit(2)
	}

	if err := logger.Init("broker", logger.LevelInfo, config.LogDirectory); err != nil {
		panic(err)
	}
	defer logger.Close()

	broker, err := broker.NewBroker(config.Address)
	if err != nil {
		logger.Fatal(
			"broker_create_failed",
			logger.Err(err),
		)
	}

	defer broker.Close()

	for _, topic := range topics {
		if err := broker.CreateTopic(topic.Name, filepath.Join(config.DataDirectory, topic.Name), topic.Partitions, config.MaxSegmentSize); err != nil {
			logger.Fatal(
				"topic_create_failed",
				logger.Str("topic", topic.Name),
				logger.Err(err),
			)
		}
	}

	logger.Info(
		"broker_started",
		logger.Str("address", config.Address),
	)

	if err = broker.Start(); err != nil {
		logger.Fatal(
			"broker_start_failed",
			logger.Err(err),
		)
	}
}
