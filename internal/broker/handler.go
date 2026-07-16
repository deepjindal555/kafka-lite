package broker

import (
	"encoding/binary"
	"errors"
	"math"

	"kafka-lite/internal/logger"
	"kafka-lite/internal/partition"
	"kafka-lite/internal/protocol"
	"kafka-lite/internal/storage"
)

func (broker *Broker) handleProduce(request *protocol.Request) (*protocol.Response, error) {
	topic, ok := broker.getTopic(request.Produce.Topic)
	if !ok {
		return &protocol.Response{
			Type:       protocol.ResponseProduce,
			StatusCode: protocol.StatusTopicNotFound,
			Payload:    nil,
		}, nil
	}

	partition := topic.Partitions[0]

	offset, err := partition.AppendBatch(request.Produce.Batch)
	if err != nil {
		logger.Error(
			"message_produce_failed",
			logger.Str("client", request.ClientInstance),
			logger.Str("topic", request.Produce.Topic),
			logger.Err(err),
		)

		return &protocol.Response{
			Type:       protocol.ResponseProduce,
			StatusCode: mapStatusCode(err),
			Payload:    nil,
		}, nil
	}

	logger.Info(
		"message_produced",
		logger.Str("client", request.ClientInstance),
		logger.Str("topic", request.Produce.Topic),
		logger.Uint64("base_offset", offset),
		logger.Uint32("record_count", request.Produce.Batch.RecordCount),
		logger.Int("records_size", len(request.Produce.Batch.EncodedRecords)),
	)

	payload := make([]byte, protocol.OffsetFieldSize)
	protocol.PutOffset(payload, offset)

	return &protocol.Response{
		Type:       protocol.ResponseProduce,
		StatusCode: protocol.StatusOK,
		Payload:    payload,
	}, nil
}

func (broker *Broker) handleFetch(request *protocol.Request) (*protocol.Response, error) {
	topic, ok := broker.getTopic(request.Fetch.Topic)
	if !ok {
		return &protocol.Response{
			Type:       protocol.ResponseFetch,
			StatusCode: protocol.StatusTopicNotFound,
			Payload:    nil,
		}, nil
	}

	partition := topic.Partitions[0]

	payload, err := partition.Read(request.Fetch.Offset)
	if err != nil {
		statusCode := mapStatusCode(err)

		if statusCode == protocol.StatusInternalError {
			logger.Error(
				"message_fetch_failed",
				logger.Str("client", request.ClientInstance),
				logger.Str("topic", request.Fetch.Topic),
				logger.Uint64("offset", request.Fetch.Offset),
				logger.Err(err),
			)
		}

		return &protocol.Response{
			Type:       protocol.ResponseFetch,
			StatusCode: statusCode,
			Payload:    nil,
		}, nil
	}

	logger.Info(
		"message_fetched",
		logger.Str("client", request.ClientInstance),
		logger.Str("topic", request.Fetch.Topic),
		logger.Uint64("offset", request.Fetch.Offset),
		logger.Int("size", len(payload)),
	)

	return &protocol.Response{
		Type:       protocol.ResponseFetch,
		StatusCode: protocol.StatusOK,
		Payload:    payload,
	}, nil
}

func (broker *Broker) handleMetadata(request *protocol.Request) (*protocol.Response, error) {
	topics := broker.TopicNames()

	payloadSize := protocol.TopicCountFieldSize

	for _, topic := range topics {
		if len(topic) > math.MaxUint16 {
			return nil, protocol.ErrInvalidTopic
		}

		payloadSize += protocol.TopicLengthFieldSize + len(topic)
	}

	payload := make([]byte, payloadSize)

	binary.BigEndian.PutUint16(
		payload[:protocol.TopicCountFieldSize],
		uint16(len(topics)),
	)

	offset := protocol.TopicCountFieldSize

	for _, topic := range topics {
		binary.BigEndian.PutUint16(
			payload[offset:offset+protocol.TopicLengthFieldSize],
			uint16(len(topic)),
		)

		offset += protocol.TopicLengthFieldSize
		copy(payload[offset:offset+len(topic)], topic)

		offset += len(topic)
	}

	logger.Info(
		"metadata_sent",
		logger.Str("client", request.ClientInstance),
		logger.Int("topic_count", len(topics)),
	)

	return &protocol.Response{
		Type:       protocol.ResponseMetadata,
		StatusCode: protocol.StatusOK,
		Payload:    payload,
	}, nil
}

func mapStatusCode(err error) protocol.StatusCode {
	switch {
	case errors.Is(err, storage.ErrOffsetNotFound):
		return protocol.StatusOffsetNotFound

	case errors.Is(err, partition.ErrBatchTooLarge):
		return protocol.StatusBatchTooLarge

	default:
		return protocol.StatusInternalError
	}
}
