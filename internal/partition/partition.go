package partition

import (
	"errors"
	"fmt"
	"kafka-lite/internal/storage"
	"path/filepath"
	"sort"
	"sync"
)

type SegmentEntry struct {
	log   *storage.Segment
	index *storage.Index
}

type Partition struct {
	directory      string
	maxSegmentSize int64

	mu sync.RWMutex

	entries     []*SegmentEntry
	activeEntry *SegmentEntry

	nextOffset uint64
}

func OpenPartition(directory string, maxSegmentSize int64) (*Partition, error) {
	log, err := storage.OpenSegment(
		filepath.Join(directory, "000000.log"),
		0,
	)
	if err != nil {
		return nil, err
	}

	index, err := storage.OpenIndex(
		filepath.Join(directory, "000000.index"),
		0,
	)
	if err != nil {
		_ = log.Close()
		return nil, err
	}

	entry := &SegmentEntry{
		log:   log,
		index: index,
	}

	return &Partition{
		directory:      directory,
		maxSegmentSize: maxSegmentSize,

		entries:     []*SegmentEntry{entry},
		activeEntry: entry,

		nextOffset: 0,
	}, nil
}

func (partition *Partition) Append(payload []byte) (uint64, error) {
	partition.mu.Lock()
	defer partition.mu.Unlock()

	size, err := partition.activeEntry.log.Size()
	if err != nil {
		return 0, err
	}

	if !partition.activeEntry.log.CanAppend(payload, partition.maxSegmentSize-size) {
		err = partition.rotate()
		if err != nil {
			return 0, err
		}
	}

	position, err := partition.activeEntry.log.Append(payload)
	if err != nil {
		return 0, err
	}

	err = partition.activeEntry.index.Write(
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

func (partition *Partition) findSegmentEntry(offset uint64) (*SegmentEntry, error) {
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

	entry := &SegmentEntry{
		log:   log,
		index: index,
	}

	partition.entries = append(partition.entries, entry)
	partition.activeEntry = entry

	return nil
}

func (entry *SegmentEntry) Close() error {
	return errors.Join(
		entry.log.Close(),
		entry.index.Close(),
	)
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
