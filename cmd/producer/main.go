package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
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
	if err := logger.Init("producer", logger.LevelInfo); err != nil {
		panic(err)
	}
	defer logger.Close()

	reader := bufio.NewReader(os.Stdin)

	for {
		connection := connect()

		err := producerLoop(connection, reader)

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

func producerLoop(connection net.Conn, reader *bufio.Reader) error {
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
