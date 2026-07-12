package main

import (
	"fmt"
	"net"
	"sync"
	"time"

	"kafka-lite/internal/batch"
	"kafka-lite/internal/logger"
	"kafka-lite/internal/protocol"
)

type Producer struct {
	connection net.Conn

	batcher *Batcher
	config  ProducerConfig

	batchMu sync.Mutex
	sendMu  sync.Mutex

	timer *time.Timer
}

func NewProducer(connection net.Conn, config ProducerConfig) *Producer {
	return &Producer{
		connection: connection,
		batcher: NewBatcher(
			config.MaxBatchRecords,
			config.MaxBatchBytes,
		),
		config: config,
	}
}

func (producer *Producer) Produce(record *batch.Record) error {
	producer.batchMu.Lock()

	hadBatch := producer.batcher.HasBatch()
	recordBatch := producer.batcher.Append(record)
	hasBatch := producer.batcher.HasBatch()

	switch {
	case !hadBatch && hasBatch:
		// First record created a new batch.
		producer.startTimerLocked()

	case recordBatch != nil && hasBatch:
		// Previous batch completed; new active batch already exists.
		producer.stopTimerLocked()
		producer.startTimerLocked()

	case recordBatch != nil && !hasBatch:
		// Current batch completed; no active batch remains.
		producer.stopTimerLocked()
	}

	producer.batchMu.Unlock()

	producer.sendMu.Lock()
	defer producer.sendMu.Unlock()

	return producer.sendBatch(recordBatch)
}

func (producer *Producer) Flush() error {
	producer.batchMu.Lock()
	recordBatch := producer.flushLocked()
	producer.batchMu.Unlock()

	producer.sendMu.Lock()
	defer producer.sendMu.Unlock()

	return producer.sendBatch(recordBatch)
}

func (producer *Producer) Close() error {
	if err := producer.Flush(); err != nil {
		_ = producer.connection.Close()
		return err
	}

	return producer.connection.Close()
}

func (producer *Producer) onLingerTimeout() {
	producer.batchMu.Lock()
	recordBatch := producer.flushLocked()
	producer.batchMu.Unlock()

	producer.sendMu.Lock()
	defer producer.sendMu.Unlock()

	if err := producer.sendBatch(recordBatch); err != nil {
		logger.Fatal(
			"batch_send_failed",
			logger.Err(err),
		)
	}
}

func (producer *Producer) startTimerLocked() {
	if producer.config.Linger <= 0 || producer.timer != nil {
		return
	}

	producer.timer = time.AfterFunc(
		producer.config.Linger,
		producer.onLingerTimeout,
	)
}

func (producer *Producer) stopTimerLocked() {
	if producer.timer == nil {
		return
	}

	producer.timer.Stop()
	producer.timer = nil
}

func (producer *Producer) flushLocked() *batch.RecordBatch {
	producer.stopTimerLocked()
	return producer.batcher.Flush()
}

func (producer *Producer) sendBatch(recordBatch *batch.RecordBatch) error {
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

	if err := protocol.WriteFrame(producer.connection, frame); err != nil {
		return err
	}

	frame, err = protocol.ReadFrame(producer.connection)
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
