package storage

import (
	"errors"
)

var (
	ErrOffsetNotFound = errors.New("offset not found")
	ErrCorruptIndex   = errors.New("corrupt index")
)
