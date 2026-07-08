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
	"kafka-lite/internal/protocol"
)

const (
	address      = "localhost:9092"
	topic        = "default"
	retryTimeout = 100 * time.Millisecond

	maxBatchRecords = 100
	maxBatchBytes   = 64 << 10 // 64 KiB
)

var ErrProducerClosed = errors.New("producer closed")

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

		if errors.Is(err, ErrProducerClosed) {
			return
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

func flushBatch(connection net.Conn, batcher *Batcher) error {
	recordBatch := batcher.Flush()
	if recordBatch == nil {
		return nil
	}

	request := &protocol.Request{
		Type:           protocol.RequestProduce,
		ClientInstance: logger.Instance(),
		Produce: &protocol.ProduceRequest{
			Topic: topic,
			Batch: recordBatch,
		},
	}

	frame, err := protocol.EncodeRequest(request)
	if err != nil {
		logger.Fatal(
			"request_encode_failed",
			logger.Err(err),
		)
	}

	if err := protocol.WriteFrame(connection, frame); err != nil {
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
			"batch_produce_failed",
			logger.Str("status", response.StatusCode.String()),
			logger.Str("topic", topic),
		)

		fmt.Printf("Produce failed: %v\n", response.StatusCode)
		return nil
	}

	offset := protocol.GetOffset(response.Payload)

	batchSize, _ := batch.EncodedBatchSize(recordBatch)
	logger.Info(
		"batch_produced",
		logger.Uint64("base_offset", offset),
		logger.Uint32("record_count", recordBatch.RecordCount),
		logger.Int("encoded_batch_size", batchSize),
	)

	fmt.Printf(
		"Stored batch at base offset %d (%d records)\n",
		offset,
		recordBatch.RecordCount,
	)

	return nil
}

func producerLoop(connection net.Conn, reader *bufio.Reader) error {
	batcher := NewBatcher(
		maxBatchRecords,
		maxBatchBytes,
	)

	for {
		fmt.Print("> ")

		message, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				if err := flushBatch(connection, batcher); err != nil {
					return err
				}
				return nil
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

		if !batcher.CanAdd(record) {
			if err := flushBatch(connection, batcher); err != nil {
				return err
			}
		}

		if err := batcher.Add(record); err != nil {
			logger.Fatal(
				"batch_add_failed",
				logger.Err(err),
			)
		}

		if batcher.ShouldFlush() {
			if err := flushBatch(connection, batcher); err != nil {
				return err
			}
		}
	}
}
