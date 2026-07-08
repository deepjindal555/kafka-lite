package batch

import (
	"encoding/binary"
	"hash/crc32"
	"math"
)

func validate(recordBatch *RecordBatch) (int, error) {
	if recordBatch == nil {
		return 0, ErrNilBatch
	}

	if recordBatch.EncodedRecords == nil {
		return 0, ErrNilBatchData
	}

	if recordBatch.RecordCount == 0 {
		return 0, ErrInvalidRecordCount
	}

	if recordBatch.FirstTimestamp > recordBatch.MaxTimestamp {
		return 0, ErrInvalidTimestampRange
	}

	if recordBatch.Compression != CompressionNone {
		return 0, ErrInvalidCompression
	}

	batchLength := BatchHeaderSize + len(recordBatch.EncodedRecords)
	if batchLength > math.MaxUint32 {
		return 0, ErrInvalidBatchLength
	}

	return batchLength, nil
}

func EncodeBatch(recordBatch *RecordBatch) ([]byte, error) {
	batchLength, err := validate(recordBatch)
	if err != nil {
		return nil, err
	}

	data := make([]byte, batchLength)

	binary.BigEndian.PutUint64(
		data[BaseOffsetOffset:BatchLengthOffset],
		recordBatch.BaseOffset,
	)

	binary.BigEndian.PutUint32(
		data[BatchLengthOffset:RecordCountOffset],
		uint32(batchLength),
	)

	binary.BigEndian.PutUint32(
		data[RecordCountOffset:CRCOffset],
		recordBatch.RecordCount,
	)

	binary.BigEndian.PutUint64(
		data[FirstTimestampOffset:MaxTimestampOffset],
		uint64(recordBatch.FirstTimestamp),
	)

	binary.BigEndian.PutUint64(
		data[MaxTimestampOffset:CompressionOffset],
		uint64(recordBatch.MaxTimestamp),
	)

	data[CompressionOffset] = byte(recordBatch.Compression)

	copy(
		data[RecordsOffset:],
		recordBatch.EncodedRecords,
	)

	crc := crc32.ChecksumIEEE(
		data[FirstTimestampOffset:],
	)

	binary.BigEndian.PutUint32(
		data[CRCOffset:FirstTimestampOffset],
		crc,
	)

	return data, nil
}

func EncodedBatchSize(recordBatch *RecordBatch) (int, error) {
	return validate(recordBatch)
}
