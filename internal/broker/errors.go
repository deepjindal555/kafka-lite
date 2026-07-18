package broker

import "errors"

var (
	ErrTopicAlreadyExists    = errors.New("topic already exists")
	ErrInvalidPartition      = errors.New("invalid partition")
	ErrInvalidPartitionCount = errors.New("invalid partition count")
)
