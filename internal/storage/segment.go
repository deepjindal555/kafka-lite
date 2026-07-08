package storage

import (
	"io"
	"os"

	"kafka-lite/internal/batch"
)

type Segment struct {
	BaseOffset uint64
	file       *os.File
}

func OpenSegment(path string, baseOffset uint64) (*Segment, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_RDWR,
		0600,
	)
	if err != nil {
		return nil, err
	}

	return &Segment{
		BaseOffset: baseOffset,
		file:       file,
	}, nil
}

func (segment *Segment) CanAppend(batchSize int, availableSpace int64) bool {
	return int64(batchSize) <= availableSpace
}

func (segment *Segment) AppendBatch(recordBatch *batch.RecordBatch) ([]int64, error) {
	batchPosition, err := segment.file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	positions, err := batch.RecordPositions(
		recordBatch.EncodedRecords,
		batchPosition,
	)
	if err != nil {
		return nil, err
	}

	data, err := batch.EncodeBatch(recordBatch)
	if err != nil {
		return nil, err
	}

	n, err := segment.file.Write(data)
	if err != nil {
		return nil, err
	}
	if n != len(data) {
		return nil, io.ErrShortWrite
	}

	return positions, nil
}

func (segment *Segment) ReadAt(batchPosition int64) ([]byte, error) {
	record, err := segment.readRecordAt(batchPosition)
	if err != nil {
		return nil, err
	}

	return record.Payload, nil
}

func (segment *Segment) readRecordAt(batchPosition int64) (*batch.Record, error) {
	header := make([]byte, batch.RecordHeaderSize)

	n, err := segment.file.ReadAt(header, batchPosition)
	if err != nil {
		return nil, err
	}
	if n != batch.RecordHeaderSize {
		return nil, io.ErrUnexpectedEOF
	}

	recordSize, err := batch.DecodeRecordSize(header)
	if err != nil {
		return nil, err
	}

	size, err := segment.Size()
	if err != nil {
		return nil, err
	}

	if batchPosition+int64(recordSize) > size {
		return nil, io.ErrUnexpectedEOF
	}

	data := make([]byte, int(recordSize))

	n, err = segment.file.ReadAt(data, batchPosition)
	if err != nil {
		return nil, err
	}
	if n != len(data) {
		return nil, io.ErrUnexpectedEOF
	}

	return batch.DecodeRecord(data)
}

func (segment *Segment) Size() (int64, error) {
	info, err := segment.file.Stat()
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}

func (segment *Segment) Close() error {
	return segment.file.Close()
}
