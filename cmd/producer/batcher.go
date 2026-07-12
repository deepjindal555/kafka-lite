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

// Append adds a record to the active batch.
// If a batch becomes complete, ownership of it is transferred to the caller.
func (batcher *Batcher) Append(record *batch.Record) *batch.RecordBatch {
	if batcher.batch == nil {
		batcher.newBatch(record)
		return nil
	}

	if !batcher.canAppend(record) {
		recordBatch := batcher.batch
		batcher.newBatch(record)

		return recordBatch
	}

	batcher.appendRecord(record)

	if batcher.shouldFlush() {
		recordBatch := batcher.batch
		batcher.batch = nil

		return recordBatch
	}

	return nil
}

func (batcher *Batcher) Flush() *batch.RecordBatch {
	recordBatch := batcher.batch
	batcher.batch = nil

	return recordBatch
}

func (batcher *Batcher) canAppend(record *batch.Record) bool {
	if batcher.batch.RecordCount >= batcher.maxRecords {
		return false
	}

	currentBatchSize, err := batch.EncodedBatchSize(batcher.batch)
	if err != nil {
		panic(err)
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

func (batcher *Batcher) shouldFlush() bool {
	if batcher.batch.RecordCount >= batcher.maxRecords {
		return true
	}

	batchSize, err := batch.EncodedBatchSize(batcher.batch)
	if err != nil {
		panic(err)
	}

	return batchSize >= int(batcher.maxBytes)
}

func (batcher *Batcher) newBatch(record *batch.Record) {
	batcher.batch = &batch.RecordBatch{
		FirstTimestamp: record.Timestamp,
		MaxTimestamp:   record.Timestamp,
		Compression:    batch.CompressionNone,
	}

	batcher.appendRecord(record)
}

func (batcher *Batcher) appendRecord(record *batch.Record) {
	batcher.batch.RecordCount++
	batcher.batch.MaxTimestamp = record.Timestamp

	batcher.batch.EncodedRecords = append(
		batcher.batch.EncodedRecords,
		batch.EncodeRecord(record)...,
	)
}
