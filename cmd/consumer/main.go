package main

import (
	"fmt"
	"net"
	"time"

	"kafka-lite/internal/logger"
	"kafka-lite/internal/protocol"
)

const (
	address      = "localhost:9092"
	topic        = "default"
	retryTimeout = 100 * time.Millisecond
)

func main() {
	if err := logger.Init("consumer", logger.LevelInfo); err != nil {
		panic(err)
	}
	defer logger.Close()

	connection, err := net.Dial("tcp", address)
	if err != nil {
		logger.Fatal(
			"broker_connection_failed",
			logger.Str("address", address),
			logger.Err(err),
		)
	}
	defer connection.Close()

	logger.Info(
		"broker_connected",
		logger.Str("address", address),
		logger.Str("instance", logger.Instance()),
	)

	var nextOffset uint64

	for {
		request := &protocol.Request{
			Type:           protocol.RequestFetch,
			ClientInstance: logger.Instance(),
			Topic:          topic,
			Offset:         nextOffset,
		}

		frame, err := protocol.EncodeRequest(request)
		if err != nil {
			logger.Fatal(
				"request_encode_failed",
				logger.Err(err),
			)
		}

		if err = protocol.WriteFrame(connection, frame); err != nil {
			logger.Fatal(
				"request_send_failed",
				logger.Err(err),
			)
		}

		frame, err = protocol.ReadFrame(connection)
		if err != nil {
			logger.Fatal(
				"response_read_failed",
				logger.Err(err),
			)
		}

		response, err := protocol.DecodeResponse(frame)
		if err != nil {
			logger.Fatal(
				"response_decode_failed",
				logger.Err(err),
			)
		}

		switch response.StatusCode {
		case protocol.StatusOK:
			logger.Info(
				"message_fetched",
				logger.Uint64("offset", nextOffset),
				logger.Int("size", len(response.Payload)),
			)

			fmt.Printf("[%d] %s\n", nextOffset, string(response.Payload))
			nextOffset++

		case protocol.StatusOffsetNotFound:
			time.Sleep(retryTimeout)

		default:
			logger.Fatal(
				"message_fetch_failed",
				logger.Int("status", int(response.StatusCode)),
			)
		}
	}
}
