package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strings"
	"time"

	"kafka-lite/internal/batch"
	"kafka-lite/internal/logger"
	"kafka-lite/internal/protocol"
)

const (
	defaultAddress = "localhost:9092"

	defaultMaxBatchRecords = 100
	defaultMaxBatchBytes   = 64 << 10 // 64 KiB
	defaultLinger          = 10 * time.Millisecond

	retryTimeout = 100 * time.Millisecond
)

type ProducerConfig struct {
	Address string

	MaxBatchRecords uint32
	MaxBatchBytes   uint32
	Linger          time.Duration

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
		connection := connect(config.Address)

		metadata, err := fetchMetadata(connection)
		if err != nil {
			if err := connection.Close(); err != nil {
				logger.Warn(
					"connection_close_failed",
					logger.Err(err),
				)
			}

			logger.Warn(
				"metadata_fetch_failed",
				logger.Err(err),
			)

			time.Sleep(retryTimeout)
			continue
		}

		logger.Info(
			"metadata_received",
			logger.Int("topic_count", len(metadata)),
		)

		topics := extractTopicNames(metadata)

		producer := NewProducer(connection, config, topics)

		err = manualProducerLoop(producer, topics, reader)

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
			logger.Str("address", config.Address),
			logger.Err(err),
		)

		time.Sleep(retryTimeout)
	}
}

func runAutomaticProducer(config *CLIConfig) {
	connection := connect(config.Producer.Address)

	metadata, err := fetchMetadata(connection)
	if err != nil {
		_ = connection.Close()

		logger.Fatal(
			"metadata_fetch_failed",
			logger.Err(err),
		)
	}

	logger.Info(
		"metadata_received",
		logger.Int("topic_count", len(metadata)),
	)

	topics := extractTopicNames(metadata)

	producer := NewProducer(connection, config.Producer, topics)

	logger.Info(
		"automatic_workload_started",
		logger.Uint64("messages", config.Workload.Messages),
		logger.Str("mode", string(config.Workload.Mode)),
		logger.Uint64("rate", config.Workload.Rate),
	)

	if err := automaticProducerLoop(producer, config.Workload, topics); err != nil {
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

func connect(address string) net.Conn {
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

func fetchMetadata(connection net.Conn) ([]protocol.TopicMetadata, error) {
	request := &protocol.Request{
		Type:           protocol.RequestMetadata,
		ClientInstance: logger.Instance(),
		Metadata:       &protocol.MetadataRequest{},
	}

	data, err := protocol.EncodeRequest(request)
	if err != nil {
		return nil, err
	}

	if err := protocol.WriteFrame(connection, data); err != nil {
		return nil, err
	}

	data, err = protocol.ReadFrame(connection)
	if err != nil {
		return nil, err
	}

	response, err := protocol.DecodeResponse(data)
	if err != nil {
		return nil, err
	}

	if response.Type != protocol.ResponseMetadata {
		return nil, protocol.ErrUnexpectedResponse
	}

	if response.StatusCode != protocol.StatusOK {
		return nil, fmt.Errorf("metadata request failed with status %s", response.StatusCode.String())
	}

	if response.Metadata == nil {
		return nil, protocol.ErrInvalidMetadataResponse
	}

	return response.Metadata.Topics, nil
}

func extractTopicNames(metadata []protocol.TopicMetadata) []string {
	topics := make([]string, len(metadata))

	for i, topic := range metadata {
		topics[i] = topic.Name
	}

	return topics
}

func manualProducerLoop(producer *Producer, topics []string, reader *bufio.Reader) error {
	fmt.Println("Available topics:")

	for _, topic := range topics {
		fmt.Printf("  %s\n", topic)
	}

	fmt.Println()
	fmt.Println("Enter messages as:")
	fmt.Println("topic> message")
	fmt.Println()

	for {
		fmt.Print("> ")

		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return errProducerClosed
			}

			logger.Fatal(
				"message_read_failed",
				logger.Err(err),
			)
		}

		line = strings.TrimRight(line, "\r\n")

		topic, message, ok := strings.Cut(line, ">")
		if !ok {
			fmt.Println("Invalid input. Expected: <topic> > <message>")
			continue
		}

		topic = strings.TrimSpace(topic)
		message = strings.TrimSpace(message)

		if !slices.Contains(topics, topic) {
			logger.Warn(
				"unknown_topic",
				logger.Str("topic", topic),
			)
			continue
		}

		record := &batch.Record{
			Timestamp: time.Now().UnixNano(),
			Payload:   []byte(message),
		}

		if err := producer.Produce(topic, record); err != nil {
			return err
		}
	}
}
