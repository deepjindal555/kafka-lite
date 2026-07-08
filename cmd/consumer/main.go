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

	var nextOffset uint64
	for {
		connection := connect()

		err := consumerLoop(connection, &nextOffset)

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

func consumerLoop(connection net.Conn, nextOffset *uint64) error {
	for {
		request := &protocol.Request{
			Type:           protocol.RequestFetch,
			ClientInstance: logger.Instance(),
			Fetch: &protocol.FetchRequest{
				Topic:  topic,
				Offset: *nextOffset,
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

		switch response.StatusCode {
		case protocol.StatusOK:
			logger.Info(
				"message_fetched",
				logger.Uint64("offset", *nextOffset),
				logger.Int("size", len(response.Payload)),
			)

			fmt.Printf("[%d] %s\n", *nextOffset, string(response.Payload))
			(*nextOffset)++

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
