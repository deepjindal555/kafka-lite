package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"

	"kafka-lite/internal/logger"
	"kafka-lite/internal/protocol"
)

const (
	address      = "localhost:9092"
	retryTimeout = 100 * time.Millisecond
)

type ConsumerConfig struct {
	Topic string
}

func main() {
	config := ConsumerConfig{}
	flag.StringVar(&config.Topic, "topic", "", "topic")
	flag.Parse()

	if config.Topic == "" {
		fmt.Fprintln(os.Stderr, "consumer: --topic is required")
		flag.PrintDefaults()
		os.Exit(2)
	}

	if err := logger.Init(fmt.Sprintf("consumer-%s", config.Topic), logger.LevelInfo); err != nil {
		panic(err)
	}
	defer logger.Close()

	for {
		connection := connect()

		metadata, err := fetchMetadata(connection)
		if err != nil {
			_ = connection.Close()

			logger.Warn(
				"metadata_fetch_failed",
				logger.Err(err),
			)

			time.Sleep(retryTimeout)
			continue
		}

		var partitionCount uint32

		for _, topic := range metadata {
			if topic.Name == config.Topic {
				partitionCount = topic.PartitionCount
				break
			}
		}

		if partitionCount == 0 {
			logger.Fatal(
				"topic_not_found",
				logger.Str("topic", config.Topic),
			)
		}

		logger.Info(
			"metadata_received",
			logger.Str("topic", config.Topic),
			logger.Uint32("partition_count", partitionCount),
		)

		nextOffsets := make([]uint64, partitionCount)

		err = consumerLoop(connection, config.Topic, nextOffsets)

		_ = connection.Close()

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

func consumerLoop(connection net.Conn, topic string, nextOffsets []uint64) error {
	for {
		request := &protocol.Request{
			Type:           protocol.RequestFetch,
			ClientInstance: logger.Instance(),
			Fetch: &protocol.FetchRequest{
				Topic:   topic,
				Offsets: nextOffsets,
			},
		}

		frame, err := protocol.EncodeRequest(request)
		if err != nil {
			logger.Fatal(
				"request_encode_failed",
				logger.Err(err),
			)
		}

		if err = protocol.WriteFrame(connection, frame); err != nil {
			return err
		}

		frame, err = protocol.ReadFrame(connection)
		if err != nil {
			return err
		}

		response, err := protocol.DecodeResponse(frame)
		if err != nil {
			logger.Fatal(
				"response_decode_failed",
				logger.Err(err),
			)
		}

		if response.Type != protocol.ResponseFetch {
			return protocol.ErrUnexpectedResponse
		}

		switch response.StatusCode {
		case protocol.StatusOK:
			if response.Fetch == nil {
				logger.Fatal(
					"invalid_fetch_response",
					logger.Str("topic", topic),
					logger.Str("status", response.StatusCode.String()),
				)
			}

			if len(response.Fetch.Records) == 0 {
				time.Sleep(retryTimeout)
				continue
			}

			totalBytes := 0
			for _, record := range response.Fetch.Records {
				totalBytes += len(record.Record)
			}

			logger.Info(
				"records_fetched",
				logger.Str("topic", topic),
				logger.Uint32("record_count", uint32(len(response.Fetch.Records))),
				logger.Int("bytes", totalBytes),
			)

			for _, record := range response.Fetch.Records {
				offset := nextOffsets[record.Partition]

				fmt.Printf(
					"[P%d][%d] %s\n",
					record.Partition,
					offset,
					string(record.Record),
				)

				nextOffsets[record.Partition]++
			}

		case protocol.StatusTopicNotFound:
			logger.Fatal(
				"topic_not_found",
				logger.Str("topic", topic),
				logger.Str("status", response.StatusCode.String()),
			)

		default:
			logger.Fatal(
				"message_fetch_failed",
				logger.Str("topic", topic),
				logger.Str("status", response.StatusCode.String()),
			)
		}
	}
}
