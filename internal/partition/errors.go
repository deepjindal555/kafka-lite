package partition

import "errors"

var (
	ErrMissingIndex  = errors.New("missing index file")
	ErrBatchTooLarge = errors.New("batch exceeds maximum segment size")
)
