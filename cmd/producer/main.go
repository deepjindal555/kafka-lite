package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"kafka-lite/internal/batch"
	"kafka-lite/internal/logger"
)

const (
	address = "localhost:9092"
	topic   = "default"

	maxBatchRecords = 100
	maxBatchBytes   = 64 << 10 // 64 KiB
	linger          = 5 * time.Millisecond

	retryTimeout = 100 * time.Millisecond
)

type ProducerConfig struct {
	MaxBatchRecords uint32
	MaxBatchBytes   uint32

	Linger time.Duration

	PrintBatchAcks bool
}

var errProducerClosed = errors.New("producer closed")

func main() {
	config, err := parseProducerConfig(os.Args[1:])
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}

		fmt.Fprintf(os.Stderr, "producer: %v\n", err)
		os.Exit(2)
	}

	if err := logger.Init("producer", logger.LevelInfo); err != nil {
		panic(err)
	}
	defer logger.Close()

	if config.Automatic {
		runAutomaticProducer(config)
		return
	}

	runManualProducer(config.Producer)
}

func runManualProducer(config ProducerConfig) {
	reader := bufio.NewReader(os.Stdin)

	for {
		connection := connect()

		producer := NewProducer(connection, config)

		err := producerLoop(producer, reader)

		if errors.Is(err, errProducerClosed) {
			if err := producer.Close(); err != nil {
				logger.Warn(
					"producer_close_failed",
					logger.Err(err),
				)
			}
			return
		}

		if err := connection.Close(); err != nil {
			logger.Warn(
				"connection_close_failed",
				logger.Err(err),
			)
		}

		logger.Warn(
			"broker_disconnected",
			logger.Str("address", address),
			logger.Err(err),
		)

		time.Sleep(retryTimeout)
	}
}

func runAutomaticProducer(config *CLIConfig) {
	connection := connect()
	producer := NewProducer(connection, config.Producer)

	logger.Info(
		"automatic_workload_started",
		logger.Uint64("messages", config.Workload.Messages),
		logger.Str("mode", string(config.Workload.Mode)),
		logger.Uint64("rate", config.Workload.Rate),
	)

	if err := automaticProducerLoop(producer, config.Workload); err != nil {
		_ = connection.Close()
		logger.Fatal(
			"automatic_workload_failed",
			logger.Err(err),
		)
	}

	if err := producer.Close(); err != nil {
		logger.Fatal(
			"producer_close_failed",
			logger.Err(err),
		)
	}

	logger.Info(
		"automatic_workload_completed",
		logger.Uint64("messages", config.Workload.Messages),
		logger.Str("mode", string(config.Workload.Mode)),
	)
}

func connect() net.Conn {
	var reconnect bool

	for {
		connection, err := net.Dial("tcp", address)
		if err == nil {
			if reconnect {
				logger.Info(
					"broker_connected",
					logger.Str("address", address),
					logger.Bool("reconnect", true),
				)
			} else {
				logger.Info(
					"broker_connected",
					logger.Str("address", address),
					logger.Bool("reconnect", false),
					logger.Str("instance", logger.Instance()),
				)
			}

			return connection
		}

		if !reconnect {
			logger.Warn(
				"broker_connection_failed",
				logger.Str("address", address),
				logger.Err(err),
			)

			reconnect = true
		}

		time.Sleep(retryTimeout)
	}
}

func producerLoop(producer *Producer, reader *bufio.Reader) error {
	for {
		fmt.Print("> ")

		message, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return errProducerClosed
			}

			logger.Fatal(
				"message_read_failed",
				logger.Err(err),
			)
		}

		message = strings.TrimRight(message, "\r\n")

		record := &batch.Record{
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte(message),
		}

		if err := producer.Produce(record); err != nil {
			return err
		}
	}
}
