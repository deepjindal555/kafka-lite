package main

import (
	"bufio"
	"errors"
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
}

var errProducerClosed = errors.New("producer closed")

func main() {
	if err := logger.Init("producer", logger.LevelInfo); err != nil {
		panic(err)
	}
	defer logger.Close()

	reader := bufio.NewReader(os.Stdin)

	for {
		connection := connect()

		producer := NewProducer(
			connection,
			ProducerConfig{
				MaxBatchRecords: maxBatchRecords,
				MaxBatchBytes:   maxBatchBytes,
				Linger:          linger,
			},
		)

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
