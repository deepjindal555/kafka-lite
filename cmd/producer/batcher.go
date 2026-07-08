package main

import (
	"kafka-lite/internal/batch"
)

type Batcher struct {
	batch *batch.RecordBatch

	maxRecords uint32
	maxBytes   uint32
}

func NewBatcher(maxRecords, maxBytes uint32) *Batcher {
	return &Batcher{
		maxRecords: maxRecords,
		maxBytes:   maxBytes,
	}
}

func (batcher *Batcher) HasBatch() bool {
	return batcher.batch != nil
}

func (batcher *Batcher) CanAdd(record *batch.Record) bool {
	if batcher.batch == nil {
		return true
	}

	if batcher.batch.RecordCount >= batcher.maxRecords {
		return false
	}

	currentBatchSize, err := batch.EncodedBatchSize(batcher.batch)
	if err != nil {
		return false
	}

	recordSize := batch.RecordHeaderSize + len(record.Payload)

	if currentBatchSize+recordSize > int(batcher.maxBytes) {
		// Allow a single oversized record to form its own batch.
		if batcher.batch.RecordCount == 0 {
			return true
		}

		return false
	}

	return true
}

func (batcher *Batcher) Add(record *batch.Record) error {
	if batcher.batch == nil {
		batcher.batch = &batch.RecordBatch{
			FirstTimestamp: record.Timestamp,
			MaxTimestamp:   record.Timestamp,
			Compression:    batch.CompressionNone,
			EncodedRecords: make([]byte, 0),
		}
	}

	batcher.batch.RecordCount++
	batcher.batch.MaxTimestamp = record.Timestamp

	batcher.batch.EncodedRecords = append(
		batcher.batch.EncodedRecords,
		batch.EncodeRecord(record)...,
	)

	return nil
}

func (batcher *Batcher) ShouldFlush() bool {
	if batcher.batch == nil {
		return false
	}

	if batcher.batch.RecordCount >= batcher.maxRecords {
		return true
	}

	batchSize, err := batch.EncodedBatchSize(batcher.batch)
	if err != nil {
		return false
	}

	return batchSize >= int(batcher.maxBytes)
}

func (batcher *Batcher) Flush() *batch.RecordBatch {
	if batcher.batch == nil {
		return nil
	}

	recordBatch := batcher.batch
	batcher.batch = nil

	return recordBatch
}
