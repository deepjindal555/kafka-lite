package partition

import (
	"errors"
	"fmt"
	"kafka-lite/internal/storage"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type recoveredSegment struct {
	baseOffset uint64

	logPath   string
	indexPath string
}

func recoverPartition(directory string, maxSegmentSize int64) (*Partition, error) {
	segments, err := discoverSegments(directory)
	if err != nil {
		return nil, err
	}

	if len(segments) == 0 {
		return createPartition(directory, maxSegmentSize)
	}

	return recoverPartitionFromDisk(
		directory,
		maxSegmentSize,
		segments,
	)
}

func parseBaseOffset(filename string) (uint64, error) {
	base := strings.TrimSuffix(
		filename,
		filepath.Ext(filename),
	)

	return strconv.ParseUint(base, 10, 64)
}

func discoverSegments(directory string) ([]recoveredSegment, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	var segments []recoveredSegment

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if filepath.Ext(entry.Name()) != ".log" {
			continue
		}

		baseOffset, err := parseBaseOffset(entry.Name())
		if err != nil {
			return nil, err
		}

		indexPath := filepath.Join(
			directory,
			fmt.Sprintf("%06d.index", baseOffset),
		)

		if _, err := os.Stat(indexPath); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf(
					"missing index file %q: %w",
					indexPath,
					ErrMissingIndex,
				)
			}

			return nil, err
		}

		segments = append(segments, recoveredSegment{
			baseOffset: baseOffset,
			logPath:    filepath.Join(directory, entry.Name()),
			indexPath:  indexPath,
		})
	}

	return segments, nil
}

func closeRecoveredEntries(entries []*segmentEntry) {
	for _, entry := range entries {
		_ = entry.Close()
	}
}

func recoverNextOffset(entry *segmentEntry) (uint64, error) {
	size, err := entry.index.Size()
	if err != nil {
		return 0, err
	}

	entryCount := size / storage.IndexEntrySize

	return entry.log.BaseOffset + uint64(entryCount), nil
}

func recoverPartitionFromDisk(directory string, maxSegmentSize int64, segments []recoveredSegment) (*Partition, error) {
	sort.Slice(
		segments,
		func(i, j int) bool {
			return segments[i].baseOffset < segments[j].baseOffset
		},
	)

	entries := make([]*segmentEntry, 0, len(segments))

	for _, segment := range segments {
		log, err := storage.OpenSegment(
			segment.logPath,
			segment.baseOffset,
		)
		if err != nil {
			closeRecoveredEntries(entries)
			return nil, err
		}

		index, err := storage.OpenIndex(
			segment.indexPath,
			segment.baseOffset,
		)
		if err != nil {
			_ = log.Close()
			closeRecoveredEntries(entries)
			return nil, err
		}

		entries = append(entries, &segmentEntry{
			log:   log,
			index: index,
		})
	}

	activeEntry := entries[len(entries)-1]

	nextOffset, err := recoverNextOffset(activeEntry)
	if err != nil {
		closeRecoveredEntries(entries)
		return nil, err
	}

	return &Partition{
		directory:      directory,
		maxSegmentSize: maxSegmentSize,

		entries:     entries,
		activeEntry: activeEntry,
		nextOffset:  nextOffset,
	}, nil
}

func createPartition(directory string, maxSegmentSize int64) (*Partition, error) {
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

	entry := &segmentEntry{
		log:   log,
		index: index,
	}

	return &Partition{
		directory:      directory,
		maxSegmentSize: maxSegmentSize,

		entries:     []*segmentEntry{entry},
		activeEntry: entry,

		nextOffset: 0,
	}, nil
}
