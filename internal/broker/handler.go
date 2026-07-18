package broker

import (
	"errors"

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
			Produce:    &protocol.ProduceResponse{},
		}, nil
	}

	offset, err := topic.AppendBatch(request.Produce.Batch)
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
			Produce:    &protocol.ProduceResponse{},
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

	return &protocol.Response{
		Type:       protocol.ResponseProduce,
		StatusCode: protocol.StatusOK,
		Produce: &protocol.ProduceResponse{
			BaseOffset: offset,
		},
	}, nil
}

func (broker *Broker) handleFetch(request *protocol.Request) (*protocol.Response, error) {
	topic, ok := broker.getTopic(request.Fetch.Topic)
	if !ok {
		return &protocol.Response{
			Type:       protocol.ResponseFetch,
			StatusCode: protocol.StatusTopicNotFound,
			Fetch:      &protocol.FetchResponse{},
		}, nil
	}

	records := make([]protocol.PartitionRecord, 0)

	for partitionID, offset := range request.Fetch.Offsets {
		partition, err := topic.getPartition(partitionID)
		if err != nil {
			return nil, err
		}

		record, err := partition.Read(offset)
		if err != nil {
			statusCode := mapStatusCode(err)

			if statusCode == protocol.StatusOffsetNotFound {
				continue
			}

			logger.Error(
				"message_fetch_failed",
				logger.Str("client", request.ClientInstance),
				logger.Str("topic", request.Fetch.Topic),
				logger.Int("partition", partitionID),
				logger.Uint64("offset", offset),
				logger.Err(err),
			)

			return &protocol.Response{
				Type:       protocol.ResponseFetch,
				StatusCode: statusCode,
				Fetch:      &protocol.FetchResponse{},
			}, nil
		}

		records = append(records, protocol.PartitionRecord{
			Partition: uint32(partitionID),
			Record:    record,
		})
	}

	if len(records) > 0 {
		logger.Info(
			"records_fetched",
			logger.Str("client", request.ClientInstance),
			logger.Str("topic", request.Fetch.Topic),
			logger.Int("records", len(records)),
		)
	}

	return &protocol.Response{
		Type:       protocol.ResponseFetch,
		StatusCode: protocol.StatusOK,
		Fetch: &protocol.FetchResponse{
			Records: records,
		},
	}, nil
}

func (broker *Broker) handleMetadata(request *protocol.Request) (*protocol.Response, error) {
	topics := broker.TopicMetadata()

	logger.Info(
		"metadata_sent",
		logger.Str("client", request.ClientInstance),
		logger.Int("topic_count", len(topics)),
	)

	return &protocol.Response{
		Type:       protocol.ResponseMetadata,
		StatusCode: protocol.StatusOK,
		Metadata: &protocol.MetadataResponse{
			Topics: topics,
		},
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
