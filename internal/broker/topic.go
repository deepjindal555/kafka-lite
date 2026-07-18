package broker

import (
	"path/filepath"
	"strconv"
	"sync"

	"kafka-lite/internal/batch"
	"kafka-lite/internal/partition"
)

type Topic struct {
	Name       string
	Partitions []*partition.Partition

	mu                 sync.Mutex
	nextPartitionIndex int
}

func NewTopic(name string, directory string, partitionCount int, maxSegmentSize int64) (*Topic, error) {
	if partitionCount <= 0 {
		return nil, ErrInvalidPartitionCount
	}

	partitions := make([]*partition.Partition, partitionCount)

	for i := range partitionCount {
		p, err := partition.OpenPartition(filepath.Join(directory, strconv.Itoa(i)), maxSegmentSize)
		if err != nil {
			closePartitions(partitions[:i])
			return nil, err
		}

		partitions[i] = p
	}

	return &Topic{
		Name:       name,
		Partitions: partitions,
	}, nil
}

func (topic *Topic) getPartition(id int) (*partition.Partition, error) {
	if id < 0 || id >= len(topic.Partitions) {
		return nil, ErrInvalidPartition
	}

	return topic.Partitions[id], nil
}

func (topic *Topic) AppendBatch(recordBatch *batch.RecordBatch) (uint64, error) {
	topic.mu.Lock()
	defer topic.mu.Unlock()

	partition := topic.Partitions[topic.nextPartitionIndex]

	offset, err := partition.AppendBatch(recordBatch)
	if err != nil {
		return 0, err
	}

	topic.nextPartitionIndex = (topic.nextPartitionIndex + 1) % len(topic.Partitions)

	return offset, nil
}

func closePartitions(partitions []*partition.Partition) {
	for _, p := range partitions {
		_ = p.Close()
	}
}
