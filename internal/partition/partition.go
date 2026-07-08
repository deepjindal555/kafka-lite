package partition

import (
	"errors"
	"fmt"
	"kafka-lite/internal/batch"
	"kafka-lite/internal/storage"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type segmentEntry struct {
	log   *storage.Segment
	index *storage.Index
}

type Partition struct {
	directory      string
	maxSegmentSize int64

	mu sync.RWMutex

	entries     []*segmentEntry
	activeEntry *segmentEntry

	nextOffset uint64
}

func OpenPartition(directory string, maxSegmentSize int64) (*Partition, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	return recoverPartition(directory, maxSegmentSize)
}

func (partition *Partition) AppendBatch(recordBatch *batch.RecordBatch) (uint64, error) {
	partition.mu.Lock()
	defer partition.mu.Unlock()

	baseOffset := partition.nextOffset
	recordBatch.BaseOffset = baseOffset

	batchSize, err := batch.EncodedBatchSize(recordBatch)
	if err != nil {
		return 0, err
	}

	if int64(batchSize) > partition.maxSegmentSize {
		return 0, ErrBatchTooLarge
	}

	size, err := partition.activeEntry.log.Size()
	if err != nil {
		return 0, err
	}

	if !partition.activeEntry.log.CanAppend(batchSize, partition.maxSegmentSize-size) {
		if err := partition.rotate(); err != nil {
			return 0, err
		}
	}

	positions, err := partition.activeEntry.log.AppendBatch(recordBatch)
	if err != nil {
		return 0, err
	}

	for i, position := range positions {
		if err := partition.activeEntry.index.Write(baseOffset+uint64(i), position); err != nil {
			return 0, err
		}
	}

	partition.nextOffset += uint64(recordBatch.RecordCount)

	return baseOffset, nil
}

func (partition *Partition) Read(offset uint64) ([]byte, error) {
	partition.mu.RLock()
	defer partition.mu.RUnlock()

	entry, err := partition.findSegmentEntry(offset)
	if err != nil {
		return nil, err
	}

	position, err := entry.index.Lookup(offset)
	if err != nil {
		return nil, err
	}

	return entry.log.ReadAt(position)
}

func (partition *Partition) Close() error {
	partition.mu.Lock()
	defer partition.mu.Unlock()

	errs := make([]error, 0, len(partition.entries))

	for _, entry := range partition.entries {
		errs = append(errs, entry.Close())
	}

	return errors.Join(errs...)
}

func (partition *Partition) findSegmentEntry(offset uint64) (*segmentEntry, error) {
	index := sort.Search(
		len(partition.entries),
		func(i int) bool {
			return partition.entries[i].log.BaseOffset > offset
		},
	)

	if index == 0 {
		return nil, storage.ErrOffsetNotFound
	}

	index--

	return partition.entries[index], nil
}

func (partition *Partition) rotate() error {
	baseOffset := partition.nextOffset

	log, err := storage.OpenSegment(
		filepath.Join(
			partition.directory,
			fmt.Sprintf("%06d.log", baseOffset),
		),
		baseOffset,
	)
	if err != nil {
		return err
	}

	index, err := storage.OpenIndex(
		filepath.Join(
			partition.directory,
			fmt.Sprintf("%06d.index", baseOffset),
		),
		baseOffset,
	)
	if err != nil {
		_ = log.Close()
		return err
	}

	entry := &segmentEntry{
		log:   log,
		index: index,
	}

	partition.entries = append(partition.entries, entry)
	partition.activeEntry = entry

	return nil
}

func (entry *segmentEntry) Close() error {
	return errors.Join(
		entry.log.Close(),
		entry.index.Close(),
	)
}
