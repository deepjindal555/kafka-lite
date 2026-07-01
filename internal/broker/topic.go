package broker

import (
	"kafka-lite/internal/partition"
)

type Topic struct {
	Name       string
	Partitions []*partition.Partition
}

func NewTopic(name string, directory string, maxSegmentSize int64) (*Topic, error) {
	newPartition, err := partition.OpenPartition(directory, maxSegmentSize)
	if err != nil {
		return nil, err
	}

	return &Topic{
		Name:       name,
		Partitions: []*partition.Partition{newPartition},
	}, nil
}
