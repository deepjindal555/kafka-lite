package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"

	"kafka-lite/internal/logger"
	"kafka-lite/internal/protocol"
)

const (
	address = "localhost:9092"
	topic   = "default"
)

func main() {
	if err := logger.Init("producer", logger.LevelInfo); err != nil {
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

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")

		message, err := reader.ReadString('\n')
		if err != nil {
			logger.Fatal(
				"message_read_failed",
				logger.Err(err),
			)
		}

		message = strings.TrimRight(message, "\r\n")

		request := &protocol.Request{
			Type:           protocol.RequestProduce,
			ClientInstance: logger.Instance(),
			Topic:          topic,
			Payload:        []byte(message),
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

		if response.StatusCode != protocol.StatusOK {
			logger.Error(
				"message_produce_failed",
				logger.Int("status", int(response.StatusCode)),
			)

			fmt.Printf("Produce failed: %v\n", response.StatusCode)
			continue
		}

		offset := protocol.GetOffset(response.Payload)

		logger.Info(
			"message_produced",
			logger.Uint64("offset", offset),
			logger.Int("size", len(message)),
		)

		fmt.Printf("Stored at offset %d\n", offset)
	}
}
