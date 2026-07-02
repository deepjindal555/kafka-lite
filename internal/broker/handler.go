package broker

import (
	"errors"

	"kafka-lite/internal/logger"
	"kafka-lite/internal/protocol"
	"kafka-lite/internal/storage"
)

func (broker *Broker) handleProduce(request *protocol.Request) (*protocol.Response, error) {
	topic, ok := broker.getTopic(request.Topic)
	if !ok {
		return &protocol.Response{
			Type:       protocol.ResponseProduce,
			StatusCode: protocol.StatusTopicNotFound,
			Payload:    nil,
		}, nil
	}

	partition := topic.Partitions[0]

	offset, err := partition.Append(request.Payload)
	if err != nil {
		logger.Error(
			"message_produce_failed",
			logger.Str("client", request.ClientInstance),
			logger.Str("topic", request.Topic),
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
		logger.Str("topic", request.Topic),
		logger.Uint64("offset", offset),
		logger.Int("size", len(request.Payload)),
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
	topic, ok := broker.getTopic(request.Topic)
	if !ok {
		return &protocol.Response{
			Type:       protocol.ResponseFetch,
			StatusCode: protocol.StatusTopicNotFound,
			Payload:    nil,
		}, nil
	}

	partition := topic.Partitions[0]

	payload, err := partition.Read(request.Offset)
	if err != nil {
		statusCode := mapStatusCode(err)

		if statusCode == protocol.StatusInternalError {
			logger.Error(
				"message_fetch_failed",
				logger.Str("client", request.ClientInstance),
				logger.Str("topic", request.Topic),
				logger.Uint64("offset", request.Offset),
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
		logger.Str("topic", request.Topic),
		logger.Uint64("offset", request.Offset),
		logger.Int("size", len(payload)),
	)

	return &protocol.Response{
		Type:       protocol.ResponseFetch,
		StatusCode: protocol.StatusOK,
		Payload:    payload,
	}, nil
}

func mapStatusCode(err error) protocol.StatusCode {
	switch {
	case errors.Is(err, storage.ErrOffsetNotFound):
		return protocol.StatusOffsetNotFound

	default:
		return protocol.StatusInternalError
	}
}
