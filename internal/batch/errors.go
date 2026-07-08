package batch

import "errors"

var (
	ErrNilBatch     = errors.New("nil batch")
	ErrNilBatchData = errors.New("nil batch data")

	ErrRecordTooSmall    = errors.New("record too small")
	ErrInvalidRecordSize = errors.New("invalid record size")
	ErrRecordCRCMismatch = errors.New("record crc mismatch")

	ErrBatchTooSmall         = errors.New("batch too small")
	ErrInvalidBatchLength    = errors.New("invalid batch length")
	ErrInvalidRecordCount    = errors.New("invalid record count")
	ErrInvalidTimestampRange = errors.New("invalid timestamp range")
	ErrInvalidCompression    = errors.New("invalid compression type")
	ErrBatchCRCMismatch      = errors.New("batch crc mismatch")
)
