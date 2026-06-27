package partition

import (
	"errors"
	"kafka-lite/internal/storage"
	"path/filepath"
	"time"
)

type Partition struct {
	segment    *storage.Segment
	index      *storage.Index
	nextOffset uint64
}

func OpenPartition(directory string) (*Partition, error) {
	segment, err := storage.OpenSegment(
		filepath.Join(directory, "000000.log"),
	)
	if err != nil {
		return nil, err
	}

	index, err := storage.OpenIndex(
		filepath.Join(directory, "000000.index"),
	)
	if err != nil {
		_ = segment.Close()
		return nil, err
	}

	return &Partition{
		segment:    segment,
		index:      index,
		nextOffset: 0,
	}, nil
}

func (partition *Partition) Append(payload []byte) (uint64, error) {
	record := &storage.Record{
		Timestamp: time.Now().UnixNano(),
		Payload:   payload,
	}

	position, err := partition.segment.Append(record)
	if err != nil {
		return 0, err
	}

	err = partition.index.Write(
		partition.nextOffset,
		position,
	)
	if err != nil {
		return 0, err
	}

	offset := partition.nextOffset
	partition.nextOffset++

	return offset, nil
}

func (partition *Partition) Read(offset uint64) ([]byte, error) {
	position, err := partition.index.Lookup(offset)
	if err != nil {
		return nil, err
	}

	record, err := partition.segment.ReadAt(position)
	if err != nil {
		return nil, err
	}
	return record.Payload, nil
}

func (partition *Partition) Close() error {
	return errors.Join(
		partition.segment.Close(),
		partition.index.Close(),
	)
}
