package batch

const (
	BatchBaseOffsetFieldSize = 8
	BatchLengthFieldSize     = 4
	RecordCountFieldSize     = 4
	BatchCRCFieldSize        = 4
	FirstTimestampFieldSize  = 8
	MaxTimestampFieldSize    = 8
	CompressionFieldSize     = 1
	ReservedFieldSize        = 3

	BatchHeaderSize = BatchBaseOffsetFieldSize +
		BatchLengthFieldSize +
		RecordCountFieldSize +
		BatchCRCFieldSize +
		FirstTimestampFieldSize +
		MaxTimestampFieldSize +
		CompressionFieldSize +
		ReservedFieldSize

	BaseOffsetOffset     = 0
	BatchLengthOffset    = BaseOffsetOffset + BatchBaseOffsetFieldSize
	RecordCountOffset    = BatchLengthOffset + BatchLengthFieldSize
	CRCOffset            = RecordCountOffset + RecordCountFieldSize
	FirstTimestampOffset = CRCOffset + BatchCRCFieldSize
	MaxTimestampOffset   = FirstTimestampOffset + FirstTimestampFieldSize
	CompressionOffset    = MaxTimestampOffset + MaxTimestampFieldSize
	ReservedOffset       = CompressionOffset + CompressionFieldSize

	RecordsOffset = ReservedOffset + ReservedFieldSize
)

type CompressionType uint8

const (
	CompressionNone CompressionType = iota
)

type RecordBatch struct {
	BaseOffset uint64

	FirstTimestamp int64
	MaxTimestamp   int64

	Compression CompressionType

	RecordCount uint32

	EncodedRecords []byte
}
