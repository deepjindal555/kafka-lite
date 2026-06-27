package storage

import (
	"errors"
)

var (
	ErrRecordTooSmall = errors.New("record too small")
	ErrInvalidSize    = errors.New("invalid record size")
	ErrCRCMismatch    = errors.New("crc mismatch")

	ErrOffsetNotFound = errors.New("offset not found")
	ErrCorruptIndex   = errors.New("corrupt index")
)
