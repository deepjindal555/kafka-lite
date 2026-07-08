package batch

import (
	"encoding/binary"
	"hash/crc32"
)

func DecodeBatch(data []byte) (*RecordBatch, error) {
	if len(data) < BatchHeaderSize {
		return nil, ErrBatchTooSmall
	}

	batchLength := binary.BigEndian.Uint32(data[BatchLengthOffset:RecordCountOffset])
	if int(batchLength) != len(data) {
		return nil, ErrInvalidBatchLength
	}

	recordCount := binary.BigEndian.Uint32(data[RecordCountOffset:CRCOffset])
	if recordCount == 0 {
		return nil, ErrInvalidRecordCount
	}

	compression := CompressionType(data[CompressionOffset])
	if compression != CompressionNone {
		return nil, ErrInvalidCompression
	}

	expectedCRC := binary.BigEndian.Uint32(data[CRCOffset:FirstTimestampOffset])
	if crc32.ChecksumIEEE(data[FirstTimestampOffset:]) != expectedCRC {
		return nil, ErrBatchCRCMismatch
	}

	firstTimestamp := int64(
		binary.BigEndian.Uint64(
			data[FirstTimestampOffset:MaxTimestampOffset],
		),
	)

	maxTimestamp := int64(
		binary.BigEndian.Uint64(
			data[MaxTimestampOffset:CompressionOffset],
		),
	)

	if firstTimestamp > maxTimestamp {
		return nil, ErrInvalidTimestampRange
	}

	batchData := make([]byte, len(data)-RecordsOffset)
	copy(batchData, data[RecordsOffset:])

	return &RecordBatch{
		BaseOffset: binary.BigEndian.Uint64(
			data[BaseOffsetOffset:BatchLengthOffset],
		),

		FirstTimestamp: firstTimestamp,
		MaxTimestamp:   maxTimestamp,

		Compression: compression,

		RecordCount: recordCount,

		EncodedRecords: batchData,
	}, nil
}
