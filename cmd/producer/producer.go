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

type topicState struct {
	batcher *Batcher
	timer   *time.Timer
}

type Producer struct {
	connection net.Conn

	topics map[string]*topicState
	config ProducerConfig

	batchMu sync.Mutex
	sendMu  sync.Mutex
}

func NewProducer(connection net.Conn, config ProducerConfig) *Producer {
	topics := make(map[string]*topicState)

	for _, topic := range config.Topics {
		topics[topic] = &topicState{
			batcher: NewBatcher(
				config.MaxBatchRecords,
				config.MaxBatchBytes,
			),
		}
	}

	return &Producer{
		connection: connection,
		topics:     topics,
		config:     config,
	}
}

func (producer *Producer) Produce(topic string, record *batch.Record) error {
	producer.batchMu.Lock()

	state, ok := producer.topics[topic]
	if !ok {
		producer.batchMu.Unlock()
		return fmt.Errorf("unknown topic %q", topic)
	}

	hadBatch := state.batcher.HasBatch()
	recordBatch := state.batcher.Append(record)
	hasBatch := state.batcher.HasBatch()

	switch {
	case !hadBatch && hasBatch:
		// First record created a new batch.
		producer.startTimerLocked(topic)

	case recordBatch != nil && hasBatch:
		// Previous batch completed; new active batch already exists.
		producer.stopTimerLocked(topic)
		producer.startTimerLocked(topic)

	case recordBatch != nil && !hasBatch:
		// Current batch completed; no active batch remains.
		producer.stopTimerLocked(topic)
	}

	producer.batchMu.Unlock()

	producer.sendMu.Lock()
	defer producer.sendMu.Unlock()

	return producer.sendBatch(topic, recordBatch)
}

func (producer *Producer) Flush() error {
	producer.batchMu.Lock()

	batches := make(map[string]*batch.RecordBatch, len(producer.topics))

	for topic := range producer.topics {
		batches[topic] = producer.flushLocked(topic)
	}

	producer.batchMu.Unlock()

	producer.sendMu.Lock()
	defer producer.sendMu.Unlock()

	for topic, recordBatch := range batches {
		if err := producer.sendBatch(topic, recordBatch); err != nil {
			return err
		}
	}

	return nil
}

func (producer *Producer) Close() error {
	if err := producer.Flush(); err != nil {
		_ = producer.connection.Close()
		return err
	}

	return producer.connection.Close()
}

func (producer *Producer) onLingerTimeout(topic string) {
	producer.batchMu.Lock()
	recordBatch := producer.flushLocked(topic)
	producer.batchMu.Unlock()

	producer.sendMu.Lock()
	defer producer.sendMu.Unlock()

	if err := producer.sendBatch(topic, recordBatch); err != nil {
		logger.Fatal(
			"batch_send_failed",
			logger.Str("topic", topic),
			logger.Uint32("record_count", recordBatch.RecordCount),
			logger.Err(err),
		)
	}
}

func (producer *Producer) startTimerLocked(topic string) {
	state := producer.topics[topic]

	if producer.config.Linger <= 0 || state.timer != nil {
		return
	}

	state.timer = time.AfterFunc(
		producer.config.Linger,
		func() {
			producer.onLingerTimeout(topic)
		},
	)
}

func (producer *Producer) stopTimerLocked(topic string) {
	state := producer.topics[topic]

	if state.timer == nil {
		return
	}

	state.timer.Stop()
	state.timer = nil
}

func (producer *Producer) flushLocked(topic string) *batch.RecordBatch {
	producer.stopTimerLocked(topic)
	return producer.topics[topic].batcher.Flush()
}

func (producer *Producer) sendBatch(topic string, recordBatch *batch.RecordBatch) error {
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

	if response.Type != protocol.ResponseProduce {
		return protocol.ErrUnexpectedResponse
	}

	if response.StatusCode != protocol.StatusOK {
		logger.Error(
			"batch_produce_failed",
			logger.Str("topic", topic),
			logger.Str("status", response.StatusCode.String()),
			logger.Uint32("record_count", recordBatch.RecordCount),
		)

		if producer.config.PrintBatchAcks {
			fmt.Printf(
				"Produce to topic %q failed: %v\n",
				topic,
				response.StatusCode,
			)
		}
		return nil
	}

	if response.Produce == nil {
		return protocol.ErrInvalidProduceResponse
	}

	offset := response.Produce.BaseOffset
	batchSize, _ := batch.EncodedBatchSize(recordBatch)

	logger.Info(
		"batch_produced",
		logger.Str("topic", topic),
		logger.Uint64("base_offset", offset),
		logger.Uint32("record_count", recordBatch.RecordCount),
		logger.Int("encoded_batch_size", batchSize),
	)

	if producer.config.PrintBatchAcks {
		fmt.Printf(
			"Stored batch for topic %q at base offset %d (%d records)\n",
			topic,
			offset,
			recordBatch.RecordCount,
		)
	}

	return nil
}
